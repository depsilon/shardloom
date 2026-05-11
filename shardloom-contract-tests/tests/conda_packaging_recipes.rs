const README: &str = include_str!("../../packaging/conda/README.md");
const CLI_RECIPE: &str = include_str!("../../packaging/conda/shardloom-cli/meta.yaml");
const PYTHON_RECIPE: &str = include_str!("../../packaging/conda/shardloom-python/meta.yaml");
const META_RECIPE: &str = include_str!("../../packaging/conda/shardloom/meta.yaml");

fn requirement_like_lines(recipe: &str) -> Vec<&str> {
    recipe
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("- "))
        .collect()
}

#[test]
fn conda_packaging_scaffold_declares_three_package_split() {
    assert!(README.contains("shardloom-cli"));
    assert!(README.contains("shardloom-python"));
    assert!(README.contains("shardloom"));
    assert!(README.contains("No package publication is authorized"));

    assert!(CLI_RECIPE.contains("name: shardloom-cli"));
    assert!(PYTHON_RECIPE.contains("name: shardloom-python"));
    assert!(META_RECIPE.contains("name: shardloom"));
    assert!(PYTHON_RECIPE.contains("noarch: python"));
    assert!(META_RECIPE.contains("noarch: generic"));
}

#[test]
fn conda_cli_recipe_builds_platform_binary_without_fallback_dependencies() {
    assert!(CLI_RECIPE.contains("cargo auditable install"));
    assert!(CLI_RECIPE.contains("--locked --no-track --bins"));
    assert!(CLI_RECIPE.contains("cargo-bundle-licenses"));
    assert!(CLI_RECIPE.contains("shardloom status --format json"));

    for line in requirement_like_lines(CLI_RECIPE) {
        assert!(!line.contains("spark"));
        assert!(!line.contains("datafusion"));
        assert!(!line.contains("duckdb"));
        assert!(!line.contains("polars"));
        assert!(!line.contains("pandas"));
        assert!(!line.contains("dask"));
        assert!(!line.contains("velox"));
    }
}

#[test]
fn conda_python_recipe_stays_noarch_and_import_only() {
    assert!(PYTHON_RECIPE.contains("{{ PYTHON }} -m pip install ./python"));
    assert!(PYTHON_RECIPE.contains("imports:"));
    assert!(PYTHON_RECIPE.contains("- shardloom"));
    assert!(PYTHON_RECIPE.contains("ShardLoomClient.from_env()"));
    assert!(!PYTHON_RECIPE.contains("smoke_check()"));

    for line in requirement_like_lines(PYTHON_RECIPE) {
        assert!(!line.contains("spark"));
        assert!(!line.contains("datafusion"));
        assert!(!line.contains("duckdb"));
        assert!(!line.contains("polars"));
        assert!(!line.contains("pandas"));
        assert!(!line.contains("dask"));
        assert!(!line.contains("velox"));
    }
}

#[test]
fn conda_metapackage_depends_only_on_cli_and_python_wrapper() {
    assert!(META_RECIPE.contains("- shardloom-cli =={{ version }}"));
    assert!(META_RECIPE.contains("- shardloom-python =={{ version }}"));
    assert!(META_RECIPE.contains("smoke_check()"));

    for line in requirement_like_lines(META_RECIPE) {
        if line.starts_with("- shardloom") || line.starts_with("- python -c") {
            continue;
        }
        assert!(!line.contains("spark"));
        assert!(!line.contains("datafusion"));
        assert!(!line.contains("duckdb"));
        assert!(!line.contains("polars"));
        assert!(!line.contains("pandas"));
        assert!(!line.contains("dask"));
        assert!(!line.contains("velox"));
    }
}
