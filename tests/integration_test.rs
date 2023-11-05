use std::env;
use std::path::PathBuf;
use assert_cmd::Command;
use test_case::test_case;
use std::fs;

fn command() -> Command {
    return Command::cargo_bin(env!("CARGO_PKG_NAME")).unwrap()
}

fn read_test_resource(file_name: &str) -> String {
    let mut file_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "resources", "test"].iter().collect();

    file_path.push(PathBuf::from(file_name));

    return fs::read_to_string(file_path)
        .expect(format!("Unable to read {} file", file_name).as_str());
}

#[test_case("simple-object")]
#[test_case("embedded-object")]
#[test_case("array-object")]
#[test_case("array-embedded-object")]
#[test_case("embedded-nullable-object")]
#[test_case("types")]
#[test_case("logical-decimal")]
fn avro2adf_test(test_case: &str){
    let mut adf = read_test_resource(format!("{}-adf.txt", test_case).as_str());
    let mut avro = read_test_resource(format!("{}-avro.json", test_case).as_str());

    adf = adf.trim().to_string();
    avro = avro.trim().to_string();

    adf = adf.replace("\r\n", "\n");

    let mut command = command();

    command
        .write_stdin(avro)
        .assert()
        .success()
        .stdout(adf);
}
