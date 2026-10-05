{
    description = "zenoh-gateway: view and drive a zenoh system from a browser over WebRTC; plus lib.crossRust, crate2nix builds that cross compile to Linux with zig";

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
            crossRustLib = import ./nix/cross-rust.nix { inherit nixpkgs rust-overlay; };
            systems = [ "aarch64-darwin" "aarch64-linux" "x86_64-linux" ];
            forAllSystems = lib.genAttrs systems;
        in {
            # lib.crossRust { system, cargoNix, crate ? null, features ? [ "default" ], crateOverrides ? { }, glibc ? "2.35" }
            #   -> { native, aarch64-linux, x86_64-linux }
            # lib.crossRustPackages { name, ...same } -> { <name>, <name>-aarch64-linux, <name>-x86_64-linux }
            # lib.eachSystem (system: ...) -> { aarch64-darwin = ...; aarch64-linux = ...; x86_64-linux = ...; }
            lib = { inherit (crossRustLib) crossRust crossRustPackages rustTargets; inherit systems; eachSystem = forAllSystems; };

            # the library through a small binary (example/): a loopback server and the Rust client (feature client)
            packages = forAllSystems (system:
                let built = crossRustLib.crossRustPackages { name = "zenoh-gateway-example"; inherit system; cargoNix = ./example/Cargo.nix; };
                in built // { default = built.zenoh-gateway-example; crate2nix = (crossRustLib.pkgsFor system).crate2nix; });

            devShells = forAllSystems (system:
                let pkgs = crossRustLib.pkgsFor system;
                in {
                    default = pkgs.mkShell {
                        packages = [
                            ((crossRustLib.toolchainFor pkgs).override { extensions = [ "rust-src" "clippy" "rustfmt" "rust-analyzer" ]; })
                            pkgs.crate2nix
                            pkgs.deno
                        ] ++ lib.optionals pkgs.stdenv.hostPlatform.isDarwin [ pkgs.libiconv ];
                    };
                });

            templates.downstream = {
                path = ./templates/downstream;
                description = "a crate using zenoh-gateway (client) and zenoh-dimos-codecs (codecs + hardware encoders), built natively and for aarch64 / x86_64 Linux with crate2nix + zig";
            };
        };
}
