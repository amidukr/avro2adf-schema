extern crate core;

use std::io;
use serde_json::value::Value;
use std::fmt::Write;

fn do_something(output: &mut String) {
    writeln!(output, "Ha-ha-ha").unwrap();
}

fn convert_avro_2_adf_schema(avro: &str) -> String {
    let mut output = String::new();

    writeln!(output, "He-he-he").unwrap();
    do_something(&mut output);

    let v: Value = serde_json::from_str(avro).unwrap();

    return output
}

fn main() -> io::Result<()>{
    let json_input = io::read_to_string(io::stdin())?;

    println!("{}", json_input);

    // println!("{}", convert_avro_2_adf_schema(json_input.as_str()));

    Ok(())
}
