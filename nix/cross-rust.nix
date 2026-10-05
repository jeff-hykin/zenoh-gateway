# crate2nix builds where every crate is its own derivation (so crates are shared by every flake that uses this helper,
# through zenoh-gateway's nixpkgs and rust-overlay pins), natively and cross compiled to Linux with zig as the C compiler and
# linker (glibc 2.35 by default: Ubuntu 22.04 / Jetson L4T 36). See README "Nix / cross compiling".
{ nixpkgs, rust-overlay }:
let
    lib = nixpkgs.lib;

    # nix system -> rust target triple
    rustTargets = {
        "aarch64-linux" = "aarch64-unknown-linux-gnu";
        "x86_64-linux" = "x86_64-unknown-linux-gnu";
    };

    pkgsFor = system: import nixpkgs { inherit system; overlays = [ rust-overlay.overlays.default ]; };

    # one toolchain (prebuilt std for every target) for native and cross builds, so build scripts and proc macros
    # are the same derivations in both
    toolchainFor = pkgs: pkgs.rust-bin.stable.latest.minimal.override { targets = lib.attrValues rustTargets; };

    # zig as `<triple>-cc`, through cargo-zigbuild's `zig cc`, which drops the flags zig rejects (cc-rs's --target, -lgcc_s, ...)
    zigToolchain = { pkgs, target, glibc }:
        let
            zigTarget = "${lib.head (lib.splitString "-" target)}-linux-gnu.${glibc}";
            envTarget = lib.replaceStrings [ "-" ] [ "_" ] target;
            tool = name: command: ''
                cat > $out/bin/${target}-${name} <<EOF
                #!${pkgs.runtimeShell}
                export PATH=${pkgs.zig}/bin:\$PATH
                export ZIG_GLOBAL_CACHE_DIR="\''${ZIG_GLOBAL_CACHE_DIR:-\''${TMPDIR:-/tmp}/zig-cache}" ZIG_LOCAL_CACHE_DIR="\''${ZIG_LOCAL_CACHE_DIR:-\''${TMPDIR:-/tmp}/zig-cache}"
                exec ${command} "\$@"
                EOF
                chmod +x $out/bin/${target}-${name}
            '';
        in pkgs.runCommand "zig-cc-${zigTarget}" {
            passthru = { targetPrefix = "${target}-"; isClang = true; isGNU = false; isZig = true; };
        } ''
            mkdir -p $out/bin $out/nix-support
            ${tool "cc" "${pkgs.cargo-zigbuild}/bin/cargo-zigbuild zig cc -- -target ${zigTarget}"}
            ${tool "c++" "${pkgs.cargo-zigbuild}/bin/cargo-zigbuild zig c++ -- -target ${zigTarget}"}
            ${tool "ar" "${pkgs.zig}/bin/zig ar"}
            ${tool "ranlib" "${pkgs.zig}/bin/zig ranlib"}
            ln -s $out/bin/${target}-cc $out/bin/${target}-gcc
            ln -s $out/bin/${target}-c++ $out/bin/${target}-g++
            # cc-rs reads CC_<triple>; plain CC stays the build platform's (build scripts); nothing to strip or patch in a static-ish ELF
            cat > $out/nix-support/setup-hook <<EOF
            export CC_${envTarget}=$out/bin/${target}-cc CXX_${envTarget}=$out/bin/${target}-c++
            export AR_${envTarget}=$out/bin/${target}-ar RANLIB_${envTarget}=$out/bin/${target}-ranlib
            export CARGO_TARGET_${lib.toUpper envTarget}_LINKER=$out/bin/${target}-cc
            dontStrip=1
            dontPatchELF=1
            EOF
        '';

    # rustc links nix's libiconv on darwin; point the binaries at the OS copy so they run on Macs without /nix
    darwinPortable = pkgs: drv: if !pkgs.stdenv.hostPlatform.isDarwin then drv else drv.overrideAttrs (old: {
        nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ pkgs.darwin.autoSignDarwinBinariesHook ];
        postFixup = (old.postFixup or "") + ''
            for binary in $out/bin/*; do
                for library in $(otool -L "$binary" | awk '/\/nix\/store\/.*libiconv/ { print $1 }'); do
                    install_name_tool -change "$library" /usr/lib/libiconv.2.dylib "$binary"
                done
            done
        '';
    });

    # crate2nix's Cargo.nix -> { native, aarch64-linux, x86_64-linux } (cross targets only on a different system)
    #   system:          the machine building (e.g. "aarch64-darwin")
    #   cargoNix:        path to the Cargo.nix from `crate2nix generate`
    #   crate:           workspace member to build (default: the root crate)
    #   features:        root features (default [ "default" ])
    #   crateOverrides:  extra per-crate buildRustCrate overrides, merged over nixpkgs' defaultCrateOverrides
    #   glibc:           oldest glibc the Linux binaries need
    crossRust = { system, cargoNix, crate ? null, features ? [ "default" ], crateOverrides ? { }, glibc ? "2.35" }:
        let
            pkgs = pkgsFor system;
            toolchain = toolchainFor pkgs;
            # the same for native and cross builds (nixpkgs' own would differ per package set, and cross would pull GCC):
            # zstd-sys from its bundled C sources, statically (nixpkgs' override links a nix libzstd dylib);
            # proc-macro-crate asks $CARGO where the manifest is, so give it the toolchain's cargo
            overrides = {
                zstd-sys = attrs: { };
                proc-macro-crate = attrs: lib.optionalAttrs (lib.versionAtLeast attrs.version "2.0") {
                    postPatch = (attrs.postPatch or "") + ''
                        substituteInPlace src/lib.rs --replace-fail 'env::var("CARGO")' 'Ok::<_, core::convert::Infallible>("${toolchain}/bin/cargo")'
                    '';
                };
            };
            withToolchain = buildPkgs: buildPkgs.buildRustCrate.override { rustc = toolchain; cargo = toolchain; };
            build = targetPkgs: buildRustCrateForPkgs:
                let
                    generate = extraOverrides: import cargoNix {
                        pkgs = targetPkgs;
                        inherit buildRustCrateForPkgs;
                        rootFeatures = features;
                        defaultCrateOverrides = targetPkgs.defaultCrateOverrides // overrides // extraOverrides // crateOverrides;
                    };
                    member = generated: if crate == null then generated.rootCrate else generated.workspaceMembers.${crate};
                    # the binaries without debug info (the dependencies keep theirs, unused by the final link)
                    rootName = (generate { }).internal.crates.${(member (generate { })).packageId}.crateName;
                    stripped = { ${rootName} = attrs: { extraRustcOpts = (attrs.extraRustcOpts or [ ]) ++ [ "-C" "strip=debuginfo" ]; }; };
                in (member (generate stripped)).build;

            native = darwinPortable pkgs (build pkgs withToolchain);

            crossTo = targetSystem:
                let
                    target = rustTargets.${targetSystem};
                    targetPkgs = import nixpkgs {
                        localSystem = system;
                        crossSystem = { config = target; };
                        overlays = [ rust-overlay.overlays.default ];
                    };
                    # a cross stdenv whose C compiler is zig (never builds nixpkgs' GCC cross toolchain)
                    zigStdenv = targetPkgs.overrideCC targetPkgs.stdenvNoCC (zigToolchain { inherit pkgs target glibc; });
                in build targetPkgs (buildPkgs:
                    if buildPkgs.stdenv.hostPlatform.config == target
                    then buildPkgs.buildRustCrate.override { stdenv = zigStdenv; rustc = toolchain; cargo = toolchain; }
                    else withToolchain buildPkgs);
        in { inherit native; } // lib.genAttrs (lib.filter (targetSystem: targetSystem != system) (lib.attrNames rustTargets)) crossTo
           // lib.optionalAttrs (rustTargets ? ${system}) { ${system} = native; };

    # packages named "<name>" and "<name>-<system>" for each Linux target
    crossRustPackages = { name, ... }@args:
        let built = crossRust (builtins.removeAttrs args [ "name" ]);
        in { ${name} = built.native; } // lib.mapAttrs' (targetSystem: drv: lib.nameValuePair "${name}-${targetSystem}" drv) (builtins.removeAttrs built [ "native" ]);
in {
    inherit crossRust crossRustPackages pkgsFor toolchainFor rustTargets;
}
