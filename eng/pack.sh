#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output="${1:-$root/artifacts/packages}"
mkdir -p "$output"
output="$(cd "$output" && pwd)"
if [[ -n "$(find "$output" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    printf '%s\n' "Каталог пакетов должен быть пустым: $output" >&2
    exit 1
fi

cd "$root"
version="$(dotnet msbuild src/BatchFlow/BatchFlow.csproj -nologo -getProperty:Version)"
dotnet restore BatchFlow.sln
dotnet build BatchFlow.sln --configuration Release --no-restore -p:ContinuousIntegrationBuild=true
dotnet test BatchFlow.sln --configuration Release --no-build --no-restore
dotnet pack BatchFlow.sln --configuration Release --no-build --no-restore --output "$output" -p:ContinuousIntegrationBuild=true
dotnet run --project eng/BatchFlow.PackageValidation --configuration Release -- "$output" "$version" --self-test

export BATCHFLOW_PACKAGE_SOURCE="$output"
cache="$(mktemp -d "${TMPDIR:-/tmp}/batchflow-smoke-cache.XXXXXXXX")"
trap 'rm -rf "$cache"' EXIT
dotnet restore eng/BatchFlow.PackageSmokeTest/BatchFlow.PackageSmokeTest.csproj \
    --configfile eng/BatchFlow.PackageSmokeTest/NuGet.Config --packages "$cache"
dotnet run --project eng/BatchFlow.PackageSmokeTest --configuration Release --no-restore
dotnet run --project samples/BatchFlow.EntityFrameworkCore.Sample --configuration Release
if [[ -n "${BATCHFLOW_POSTGRES_TESTS:-}" ]]; then
    BATCHFLOW_POSTGRES_SAMPLE="$BATCHFLOW_POSTGRES_TESTS" \
        dotnet run --project samples/BatchFlow.Postgres.Sample --configuration Release
fi

printf '%s\n' "Кандидаты $version готовы в $output. Публикация не выполнялась."
