$ErrorActionPreference = 'Stop'
$image = 'rust:1.97-bookworm@sha256:77fac8b98f9f46062bb680b6d25d5bcaabfc400143952ebc572e924bcbedc3fa'
$repoPath = (Get-Location).Path
$rustVersion = docker run --rm $image rustc --version
if ($rustVersion -notlike 'rustc 1.97.1 *') {
    throw "Unexpected Rust version: $rustVersion"
}
docker run --rm --mount "type=bind,source=$repoPath,target=/workspace" `
    --workdir /workspace $image cargo metadata --locked --no-deps --format-version 1 |
    Out-Null
