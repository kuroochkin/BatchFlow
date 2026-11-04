using System.IO.Compression;
using System.Reflection.Metadata;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml.Linq;

try
{
    if (args.Length is < 2 or > 3 || (args.Length == 3 && args[2] != "--self-test"))
    {
        throw new ArgumentException("Ожидается: <каталог пакетов> <версия> [--self-test].");
    }

    var directory = Path.GetFullPath(args[0]);
    PackageValidation.Validate(directory, args[1]);
    Console.WriteLine($"Проверены {PackageValidation.PackageIds.Length} пакетов и пакеты символов, версия {args[1]}.");
    if (args.Length == 3)
    {
        PackageValidation.SelfTest(directory, args[1]);
    }

    return 0;
}
catch (Exception exception)
{
    Console.Error.WriteLine(exception.Message);
    return 1;
}

internal static class PackageValidation
{
    internal static readonly string[] PackageIds =
    [
        "BatchFlowKit.Abstractions", "BatchFlowKit.Core", "BatchFlowKit",
        "BatchFlowKit.Postgres", "BatchFlowKit.EntityFrameworkCore",
        "BatchFlowKit.Extensions.Logging", "BatchFlowKit.Extensions.DependencyInjection",
        "BatchFlowKit.Postgres.DependencyInjection", "BatchFlowKit.EntityFrameworkCore.DependencyInjection",
        "BatchFlowKit.Testing",
    ];

    internal static void Validate(string directory, string version)
    {
        Require(Regex.IsMatch(version, @"^\d+\.\d+\.\d+(?:-[0-9A-Za-z]+(?:[.-][0-9A-Za-z]+)*)?$"), "Некорректная версия.");
        var actualFiles = Directory.GetFiles(directory).Select(Path.GetFileName).Order().ToArray();
        var expectedFiles = PackageIds.SelectMany(id => new[] { $"{id}.{version}.nupkg", $"{id}.{version}.snupkg" }).Order().ToArray();
        Require(actualFiles.SequenceEqual(expectedFiles), "Каталог должен содержать ровно 10 ожидаемых nupkg и 10 snupkg, без устаревших или посторонних файлов.");

        foreach (var id in PackageIds)
        {
            var assembly = id.Replace("BatchFlowKit", "BatchFlow", StringComparison.Ordinal);
            using var package = ZipFile.OpenRead(Path.Combine(directory, $"{id}.{version}.nupkg"));
            using var manifest = Entry(package, $"{id}.nuspec").Open();
            var nuspec = XDocument.Load(manifest);
            var ns = nuspec.Root!.Name.Namespace;
            var metadata = nuspec.Root.Element(ns + "metadata")!;
            Require(metadata.Element(ns + "id")?.Value == id, $"{id}: неверный PackageId.");
            Require(metadata.Element(ns + "version")?.Value == version, $"{id}: неверная версия.");
            Require(metadata.Element(ns + "license")?.Value == "MIT", $"{id}: не указана MIT.");
            Require(metadata.Element(ns + "license")?.Attribute("type")?.Value == "expression", $"{id}: лицензия должна быть выражением SPDX.");
            Require(!string.IsNullOrWhiteSpace(metadata.Element(ns + "authors")?.Value), $"{id}: не указан автор.");
            Require(!string.IsNullOrWhiteSpace(metadata.Element(ns + "description")?.Value), $"{id}: нет описания.");
            Require(metadata.Element(ns + "readme")?.Value == "README.md", $"{id}: не указан README.");
            Require(ReadText(Entry(package, "LICENSE")).StartsWith("MIT License", StringComparison.Ordinal), $"{id}: нет текста MIT.");
            Require(ReadText(Entry(package, "README.md")).Contains("# BatchFlowKit", StringComparison.Ordinal), $"{id}: неверный README.");
            var repository = metadata.Element(ns + "repository");
            var commit = repository?.Attribute("commit")?.Value ?? string.Empty;
            Require(repository?.Attribute("url")?.Value.TrimEnd('/') == "https://github.com/kuroochkin/BatchFlow.git", $"{id}: неверный репозиторий.");
            Require(Regex.IsMatch(commit, "^[a-fA-F0-9]{40}$"), $"{id}: нет SHA исходного commit.");
            Entry(package, $"lib/net9.0/{assembly}.dll");
            var xml = ReadText(Entry(package, $"lib/net9.0/{assembly}.xml"));
            Require(xml.Contains("<summary>", StringComparison.Ordinal), $"{id}: отсутствует XML-документация.");

            foreach (var entry in package.Entries)
            {
                Require(entry.FullName == $"{id}.nuspec" || entry.FullName is "README.md" or "LICENSE" or "[Content_Types].xml" or "_rels/.rels" or ".signature.p7s"
                    || entry.FullName.StartsWith("package/services/metadata/core-properties/", StringComparison.Ordinal)
                    || entry.FullName == $"lib/net9.0/{assembly}.dll" || entry.FullName == $"lib/net9.0/{assembly}.xml",
                    $"{id}: неожиданный файл {entry.FullName}.");
            }

            foreach (var dependency in metadata.Descendants(ns + "dependency"))
            {
                var dependencyId = dependency.Attribute("id")!.Value;
                Require(!dependencyId.StartsWith("BatchFlow.", StringComparison.OrdinalIgnoreCase) && dependencyId != "BatchFlow", $"{id}: зависимость от чужого или старого имени {dependencyId}.");
                if (dependencyId.StartsWith("BatchFlowKit", StringComparison.Ordinal))
                {
                    Require(PackageIds.Contains(dependencyId), $"{id}: неизвестный внутренний пакет {dependencyId}.");
                    Require(dependency.Attribute("version")?.Value == version, $"{id}: несогласованная версия зависимости {dependencyId}.");
                }
            }

            using var symbols = ZipFile.OpenRead(Path.Combine(directory, $"{id}.{version}.snupkg"));
            using var pdb = Entry(symbols, $"lib/net9.0/{assembly}.pdb").Open();
            using var pdbBuffer = new MemoryStream();
            pdb.CopyTo(pdbBuffer);
            pdbBuffer.Position = 0;
            using var provider = MetadataReaderProvider.FromPortablePdbStream(pdbBuffer);
            var reader = provider.GetMetadataReader();
            var sourceLink = reader.CustomDebugInformation.Select(handle => reader.GetCustomDebugInformation(handle))
                .SingleOrDefault(info => reader.GetGuid(info.Kind) == new Guid("CC110556-A091-4D38-9FEC-25AB9A351A6A"));
            Require(!sourceLink.Value.IsNil, $"{id}: нет Source Link в PDB.");
            using var document = JsonDocument.Parse(reader.GetBlobBytes(sourceLink.Value));
            var urls = document.RootElement.GetProperty("documents").EnumerateObject().Select(property => property.Value.GetString()!).ToArray();
            Require(urls.Length > 0 && urls.All(url => url.StartsWith($"https://raw.githubusercontent.com/kuroochkin/BatchFlow/{commit}/", StringComparison.Ordinal)), $"{id}: Source Link не соответствует исходному commit.");
        }
    }

    internal static void SelfTest(string source, string version)
    {
        RunCase("отсутствующая лицензия", directory => ChangePackage(directory, "BatchFlowKit", version, archive => Entry(archive, "LICENSE").Delete()));
        RunCase("старое имя зависимости", directory => ChangeDependency(directory, "id", "BatchFlow.Abstractions"));
        RunCase("другая версия зависимости", directory => ChangeDependency(directory, "version", "0.0.1"));
        RunCase("отсутствующий пакет", directory => File.Delete(Path.Combine(directory, $"BatchFlowKit.Core.{version}.nupkg")));
        RunCase("отсутствующий пакет символов", directory => File.Delete(Path.Combine(directory, $"BatchFlowKit.Core.{version}.snupkg")));
        RunCase("посторонний пакет", directory => File.Copy(Path.Combine(directory, $"BatchFlowKit.{version}.nupkg"), Path.Combine(directory, "Unexpected.nupkg")));

        void RunCase(string name, Action<string> corrupt)
        {
            var directory = Path.Combine(Path.GetTempPath(), "batchflow-package-validation-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            try
            {
                foreach (var file in Directory.GetFiles(source))
                {
                    File.Copy(file, Path.Combine(directory, Path.GetFileName(file)));
                }

                corrupt(directory);
                try
                {
                    Validate(directory, version);
                }
                catch (InvalidDataException)
                {
                    Console.WriteLine($"Регрессия проверена: {name} отклоняется.");
                    return;
                }

                throw new InvalidOperationException($"Валидатор пропустил дефект: {name}.");
            }
            finally
            {
                Directory.Delete(directory, recursive: true);
            }
        }

        void ChangeDependency(string directory, string attribute, string value) =>
            ChangePackage(directory, "BatchFlowKit.Core", version, archive =>
            {
                var entry = Entry(archive, "BatchFlowKit.Core.nuspec");
                XDocument document;
                using (var input = entry.Open())
                {
                    document = XDocument.Load(input);
                }

                document.Descendants().First(element => element.Name.LocalName == "dependency").SetAttributeValue(attribute, value);
                entry.Delete();
                using var output = archive.CreateEntry("BatchFlowKit.Core.nuspec").Open();
                document.Save(output);
            });
    }

    private static void ChangePackage(string directory, string id, string version, Action<ZipArchive> change)
    {
        using var archive = ZipFile.Open(Path.Combine(directory, $"{id}.{version}.nupkg"), ZipArchiveMode.Update);
        change(archive);
    }

    private static ZipArchiveEntry Entry(ZipArchive archive, string name) =>
        archive.GetEntry(name) ?? throw new InvalidDataException($"Отсутствует {name}.");

    private static string ReadText(ZipArchiveEntry entry)
    {
        using var reader = new StreamReader(entry.Open());
        return reader.ReadToEnd();
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidDataException(message);
        }
    }
}
