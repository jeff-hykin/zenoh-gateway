{
    description = "my-app: a crate using zenoh-web, built natively and for aarch64 / x86_64 Linux (crate2nix + zig)";

    inputs.zenoh-web.url = "github:jeff-hykin/zenoh-web";

    outputs = { self, zenoh-web }: {
        # my-app (native), my-app-aarch64-linux, my-app-x86_64-linux; every crate is shared with the other zenoh-web flakes
        packages = zenoh-web.lib.eachSystem (system: zenoh-web.lib.crossRustPackages {
            name = "my-app";
            inherit system;
            # `nix run github:jeff-hykin/zenoh-web#crate2nix -- generate` after Cargo.lock changes
            cargoNix = ./Cargo.nix;
        });
        # rust, crate2nix, deno
        devShells = zenoh-web.devShells;
    };
}
