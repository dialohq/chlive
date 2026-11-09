# The server's OCI image, built by nix2container (no Docker): `nix build .#chlive-image`, then
# `nix run .#chlive-image.copyToDockerDaemon` or `.copyTo -- docker://…` (CI pushes it to GHCR).
# libchdb, the biggest and the least often changed, is a layer of its own.
#
# It runs as chlive (65532) in /var/lib/chlive, where the cache is (`./chlive-cache`, a volume
# to keep it across restarts). Configure it with CHLIVE_* env vars, or a mounted file and
# `--config`.
{
  lib,
  runCommand,
  cacert,
  nix2container,
  chlive,
  libchdb,
}:
let
  root = runCommand "chlive-image-root" { } ''
    mkdir -p $out/etc $out/tmp $out/var/lib/chlive
    echo 'chlive:x:65532:65532:chlive:/var/lib/chlive:/sbin/nologin' > $out/etc/passwd
    echo 'chlive:x:65532:' > $out/etc/group
  '';
in
nix2container.buildImage {
  name = "ghcr.io/dialohq/chlive";
  copyToRoot = [ root ];
  perms = [
    {
      path = root;
      regex = "/tmp$";
      mode = "1777";
    }
    {
      path = root;
      regex = "/var/lib/chlive$";
      mode = "0755";
      uid = 65532;
      gid = 65532;
      uname = "chlive";
      gname = "chlive";
    }
  ];
  layers = [ (nix2container.buildLayer { deps = [ libchdb ]; }) ];
  maxLayers = 20;
  config = {
    Entrypoint = [ (lib.getExe chlive) ];
    User = "65532:65532";
    WorkingDir = "/var/lib/chlive";
    Env = [
      # librdkafka's TLS (OpenSSL); the JWKS fetches carry their own roots
      "SSL_CERT_FILE=${cacert}/etc/ssl/certs/ca-bundle.crt"
      "HOME=/var/lib/chlive"
    ];
    ExposedPorts."8080/tcp" = { };
    Volumes."/var/lib/chlive" = { };
  };
}
