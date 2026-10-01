{
    description = "zenoh-web: zenoh <-> WebRTC data channel bridge";

    inputs = {
        nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
        flake-utils.url = "github:numtide/flake-utils";
    };

    outputs = { self, nixpkgs, flake-utils }:
        flake-utils.lib.eachDefaultSystem (system:
            let
                pkgs = import nixpkgs { inherit system; };
            in {
                devShells.default = pkgs.mkShell {
                    packages = [
                        pkgs.cargo
                        pkgs.rustc
                        pkgs.rustfmt
                        pkgs.clippy
                        pkgs.rust-analyzer
                        pkgs.deno
                    ] ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [ pkgs.libiconv ];
                    RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
                };
            });
}
