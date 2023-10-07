use std::env;
use std::path::PathBuf;
use assert_cmd::Command;
use test_case::test_case;
use std::fs;

use std::fmt::{format, Write};

fn command() -> Command {
    return Command::cargo_bin(env!("CARGO_PKG_NAME")).unwrap()
}

fn read_test_resource(file_name: &str) -> String {
    let mut file_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "resources", "test"].iter().collect();

    file_path.push(PathBuf::from(file_name));

    return fs::read_to_string(file_path)
        .expect(format!("Unable to read {} file", file_name).as_str());
}

#[test_case("test"; "main test case")]
fn avro2adf_test(testCase: &str){


    let mut adf = read_test_resource(format!("{}-adf.txt", testCase).as_str());
    let mut avro = read_test_resource(format!("{}-avro.json", testCase).as_str());

    adf = adf.trim().to_string();
    avro = adf.trim().to_string();
    adf.push('\n');

    let mut command = command();
    command
        .write_stdin(avro)
        .assert()
        .success()
        .stdout(adf);
}
