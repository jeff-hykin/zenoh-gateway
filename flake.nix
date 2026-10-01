{
    description = "zenoh-web: zenoh <-> WebRTC bridge for browsers";

    inputs = {
        nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
        rust-overlay = {
            url = "github:oxalica/rust-overlay";
            inputs.nixpkgs.follows = "nixpkgs";
        };
    };

    outputs = { self, nixpkgs, rust-overlay }:
        let
            lib = nixpkgs.lib;
            systems = [ "aarch64-darwin" "aarch64-linux" "x86_64-linux" ];
            forAllSystems = lib.genAttrs systems;
            pname = "zenoh-web";
            version = "0.1.0";
            linuxTarget = "aarch64-unknown-linux-gnu";
            # glibc floor 2.35: Ubuntu 22.04 / Jetson L4T 36 (Pi OS bookworm is 2.36)
            glibcVersion = "2.35";

            # only bridge/ (the flake already holds git-tracked files only; target/ is filtered in case of a path: flake)
            src = lib.cleanSourceWith {
                src = ./bridge;
                filter = path: type: !(lib.hasInfix "/target" path);
            };

            perSystem = system:
                let
                    pkgs = import nixpkgs { inherit system; overlays = [ rust-overlay.overlays.default ]; };
                    rustToolchain = pkgs.rust-bin.stable.latest.minimal.override { targets = [ linuxTarget ]; };
                    rustPlatform = pkgs.makeRustPlatform { cargo = rustToolchain; rustc = rustToolchain; };
                    # registry crates only; the [patch.crates-io] path crates (vendor/rtc, vendor/rtc-sctp) come with src
                    cargoDeps = rustPlatform.importCargoLock { lockFile = ./bridge/Cargo.lock; };
                    isDarwin = pkgs.stdenv.hostPlatform.isDarwin;

                    native = rustPlatform.buildRustPackage {
                        inherit pname version src cargoDeps;
                        # nasm: openh264's x86 assembly
                        nativeBuildInputs = lib.optionals pkgs.stdenv.hostPlatform.isx86 [ pkgs.nasm ]
                            ++ lib.optionals isDarwin [ pkgs.darwin.autoSignDarwinBinariesHook ];
                        cargoBuildFlags = [ "--bin" pname ];
                        # unit tests run in the dev shell / CI; the e2e suites need Chrome
                        doCheck = false;
                        # rustc links nix's libiconv (an Apple libiconv build); point it at the OS copy so the
                        # binary runs on Macs without /nix (the hook re-signs it in fixup)
                        preFixup = lib.optionalString isDarwin ''
                            for library in $(otool -L $out/bin/${pname} | awk '/\/nix\/store\/.*libiconv/ { print $1 }'); do
                                install_name_tool -change "$library" /usr/lib/libiconv.2.dylib $out/bin/${pname}
                            done
                        '';
                        meta = {
                            description = "zenoh <-> WebRTC bridge for browsers";
                            license = [ lib.licenses.mit lib.licenses.asl20 ];
                            mainProgram = pname;
                        };
                    };

                    # aarch64 Linux from a Mac: cargo-zigbuild with zig as the C/C++ cross toolchain
                    crossAarch64Linux = pkgs.stdenv.mkDerivation {
                        pname = "${pname}-aarch64-linux";
                        inherit version src cargoDeps;
                        nativeBuildInputs = [ rustToolchain rustPlatform.cargoSetupHook pkgs.cargo-zigbuild pkgs.zig ];
                        # the darwin stdenv's fixup would try to otool/strip an ELF
                        dontFixup = true;
                        buildPhase = ''
                            runHook preBuild
                            export HOME=$TMPDIR
                            export ZIG_GLOBAL_CACHE_DIR=$TMPDIR/zig-cache ZIG_LOCAL_CACHE_DIR=$TMPDIR/zig-local-cache
                            cargo zigbuild --release --offline --bin ${pname} --target ${linuxTarget}.${glibcVersion}
                            runHook postBuild
                        '';
                        installPhase = ''
                            runHook preInstall
                            install -Dm755 target/${linuxTarget}/release/${pname} $out/bin/${pname}
                            runHook postInstall
                        '';
                        meta.mainProgram = pname;
                    };
                in {
                    packages = {
                        ${pname} = native;
                        default = native;
                    } // lib.optionalAttrs isDarwin {
                        "${pname}-aarch64-linux" = crossAarch64Linux;
                    };

                    devShells.default = pkgs.mkShell {
                        packages = [
                            (pkgs.rust-bin.stable.latest.default.override { extensions = [ "rust-src" "clippy" "rustfmt" "rust-analyzer" ]; targets = [ linuxTarget ]; })
                            pkgs.deno
                            pkgs.cargo-zigbuild
                            pkgs.zig
                        ] ++ lib.optionals isDarwin [ pkgs.libiconv ];
                    };
                };

            outputsBySystem = forAllSystems perSystem;
        in {
            packages = lib.mapAttrs (system: outputs: outputs.packages) outputsBySystem;
            devShells = lib.mapAttrs (system: outputs: outputs.devShells) outputsBySystem;
        };
}
