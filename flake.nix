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
            # no x86_64-darwin: nixpkgs 26.11 dropped it (Intel Macs use the release binary)
            systems = [ "aarch64-darwin" "aarch64-linux" "x86_64-linux" ];
            forAllSystems = lib.genAttrs systems;
            pname = "zenoh-web";
            version = "0.1.0";
            linuxTarget = "aarch64-unknown-linux-gnu";
            linuxX86Target = "x86_64-unknown-linux-gnu";
            darwinX86Target = "x86_64-apple-darwin";
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
                    rustToolchain = pkgs.rust-bin.stable.latest.minimal.override { targets = [ linuxTarget linuxX86Target darwinX86Target ]; };
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

                    # Linux from a Mac: cargo-zigbuild with zig as the C/C++ cross toolchain
                    crossLinux = target: pkgs.stdenv.mkDerivation {
                        pname = "${pname}-${lib.head (lib.splitString "-" target)}-linux";
                        inherit version src cargoDeps;
                        nativeBuildInputs = [ rustToolchain rustPlatform.cargoSetupHook pkgs.cargo-zigbuild pkgs.zig ]
                            ++ lib.optionals (lib.hasPrefix "x86_64" target) [ pkgs.nasm ];
                        # the darwin stdenv's fixup would try to otool/strip an ELF
                        dontFixup = true;
                        buildPhase = ''
                            runHook preBuild
                            export HOME=$TMPDIR
                            export ZIG_GLOBAL_CACHE_DIR=$TMPDIR/zig-cache ZIG_LOCAL_CACHE_DIR=$TMPDIR/zig-local-cache
                            cargo zigbuild --release --offline --bin ${pname} --target ${target}.${glibcVersion}
                            runHook postBuild
                        '';
                        installPhase = ''
                            runHook preInstall
                            install -Dm755 target/${target}/release/${pname} $out/bin/${pname}
                            runHook postInstall
                        '';
                        meta.mainProgram = pname;
                    };

                    # Intel macOS from Apple Silicon: the same clang/SDK (universal), rustc told the other arch
                    crossX86Darwin = pkgs.stdenv.mkDerivation {
                        pname = "${pname}-x86_64-darwin";
                        inherit version src cargoDeps;
                        nativeBuildInputs = [ rustToolchain rustPlatform.cargoSetupHook pkgs.nasm pkgs.darwin.autoSignDarwinBinariesHook ];
                        buildPhase = ''
                            runHook preBuild
                            export HOME=$TMPDIR
                            cargo build --release --offline --bin ${pname} --target ${darwinX86Target}
                            runHook postBuild
                        '';
                        installPhase = ''
                            runHook preInstall
                            install -Dm755 target/${darwinX86Target}/release/${pname} $out/bin/${pname}
                            runHook postInstall
                        '';
                        preFixup = native.preFixup;
                        meta.mainProgram = pname;
                    };
                in {
                    packages = {
                        ${pname} = native;
                        default = native;
                    } // lib.optionalAttrs isDarwin {
                        "${pname}-aarch64-linux" = crossLinux linuxTarget;
                        "${pname}-x86_64-linux" = crossLinux linuxX86Target;
                        "${pname}-x86_64-darwin" = crossX86Darwin;
                        # all four release binaries as result/<target-triple>/zenoh-web
                        release = pkgs.runCommand "${pname}-release-${version}" { } (lib.concatMapStrings (entry: ''
                            install -Dm755 ${entry.package}/bin/${pname} $out/${entry.target}/${pname}
                        '') [
                            { target = "aarch64-apple-darwin"; package = native; }
                            { target = darwinX86Target; package = crossX86Darwin; }
                            { target = linuxTarget; package = crossLinux linuxTarget; }
                            { target = linuxX86Target; package = crossLinux linuxX86Target; }
                        ]);
                    };

                    apps.default = { type = "app"; program = "${native}/bin/${pname}"; };

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
            apps = lib.mapAttrs (system: outputs: outputs.apps) outputsBySystem;
        };
}
