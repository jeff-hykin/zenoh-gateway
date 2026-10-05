{
    description = "my-app: a crate using zenoh-gateway, built natively and for aarch64 / x86_64 Linux (crate2nix + zig)";

    inputs.zenoh-gateway.url = "github:jeff-hykin/zenoh-gateway";

    outputs = { self, zenoh-gateway }: {
        # my-app (native), my-app-aarch64-linux, my-app-x86_64-linux; every crate is shared with the other zenoh-gateway flakes
        packages = zenoh-gateway.lib.eachSystem (system: zenoh-gateway.lib.crossRustPackages {
            name = "my-app";
            inherit system;
            # `nix run github:jeff-hykin/zenoh-gateway#crate2nix -- generate` after Cargo.lock changes
            cargoNix = ./Cargo.nix;
        });
        # rust, crate2nix, deno
        devShells = zenoh-gateway.devShells;
    };
}
