{
  description = "Develop Python on Nix with uv";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
  };

  outputs =
    { nixpkgs, ... }:
    let
      inherit (nixpkgs) lib;
      forAllSystems = lib.genAttrs lib.systems.flakeExposed;
    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          default =
            let
              python = pkgs.python312;
            in
            pkgs.mkShell {
              packages = [
                pkgs.cbc
                pkgs.cmake
                pkgs.ninja
                pkgs.openblas
                pkgs.pkg-config
                python
                pkgs.uv
              ];

              shellHook = lib.optionalString pkgs.stdenv.isLinux ''
                # Keep a minimal runtime library path for Python extensions on NixOS.
                # Avoid adding glibc here to prevent host shell crashes when using `nix develop -c $SHELL`.
                export UV_PYTHON=${python}/bin/python
                export PY_RUNTIME_LIBS="${
                    lib.makeLibraryPath [
                      pkgs.stdenv.cc.cc.lib
                      pkgs.zlib
                      pkgs.openssl
                    ]
                  }"
                export LD_LIBRARY_PATH="$PY_RUNTIME_LIBS''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
              '';
            };
        }
      );
    };
}
