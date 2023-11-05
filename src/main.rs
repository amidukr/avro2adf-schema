extern crate core;

use std::collections::HashMap;
use std::io;
use once_cell::sync::Lazy;

use serde_json::value::Value;
use serde_json::Map;

static AVRO_2_ADF_TYPES: Lazy<HashMap<&str, &str>> = Lazy::new(|| HashMap::from([
    ("boolean", "boolean"),
    ("int", "integer"),
    ("long", "long"),
    ("float", "float"),
    ("double", "double"),
    ("string", "string")
]));

trait ValueExt {
    fn as_array(&self) -> Option<&Vec<Value>>;
    fn as_object_value(&self) -> Option<&Map<String, Value>>;
    fn as_string(&self) -> Option<&String>;
}

impl ValueExt for Value {
    fn as_array(&self) -> Option<&Vec<Value>> {
        return match self {
            Value::Array(o) => Some(o),
            _ => None
        };
    }

    fn as_object_value(&self) -> Option<&Map<String, Value>> {
        return match self {
            Value::Object(o) => Some(o),
            _ => None
        };
    }

    fn as_string(&self) -> Option<&String> {
        return match self {
            Value::String(o) => Some(o),
            _ => None
        };
    }
}

fn handle_any_json_element(ident: usize, json: &Value) {
    match json {
        Value::Null => {}
        Value::String(o) => handle_json_string_type(o),
        Value::Array(o) => handle_json_array_type(ident, o),
        Value::Object(o) => handle_json_map_element(ident, o),
        _ => panic!("Unexpected avro type tag: {}", json),
    }
}

fn handle_json_map_element(ident: usize, object: &Map<String, Value>) {
    if let Some(Value::String(t)) = object.get("type") {
        match t.as_str() {
            "record" => {
                handle_avro_record_type(ident, object);
                return;
            }
            "array" => {
                handle_avro_array_type(ident, object);
                return;
            }
            "bytes" => {
                handle_avro_bytes_type(object);
                return;
            }
            _ => {}
        }
    }

    panic!("Unexpected json map element definition = {:?}", object);
}

fn handle_json_array_type(ident: usize, array: &Vec<Value>) {
    let type_array: Vec<_> = array.iter()
        .filter(|t| match t {
            Value::String(s) if s == "null" => false,
            _ => true
        }
        ).collect();

    if type_array.len() != 1 {
        panic!("Array of types has different to 1 of non-nullable types, {:?}", array);
    }

    handle_any_json_element(ident, type_array.into_iter().next()
        .unwrap())
}

fn handle_json_string_type(t: &String) {
    let adf_type = AVRO_2_ADF_TYPES.get(t.as_str())
        .expect(format!("Unexpected avro type: {}", t).as_str());

    print!("{}", adf_type)
}

fn handle_avro_record_type(ident: usize, object: &Map<String, Value>) {
    let fields = object.get("fields")
        .expect(format!("`record` type expect to have 'fields' attribute, json = {:?}", object).as_str());

    let field_list = fields.as_array()
        .expect(format!("`fields` attribute expected to be of json array type, fields = {}", fields).as_str());


    println!("(");

    for (pos, e) in field_list.iter().enumerate() {
        let field = e.as_object_value()
            .expect(format!("Elements of `fields` attributes are expected to be of json object type, fields element = {}", e).as_str());

        let name = field.get("name")
            .expect(format!("Field element expected to have json `name` attribute, fields element = {:?}", field).as_str())
            .as_str()
            .expect(format!("`name` field element expected to be of json string type, fields element = {:?}", field).as_str());

        let _type = field.get("type")
            .expect(format!("Field element expected to have json `type` attribute, field = {:?}", field).as_str());

        print!("{: <1$}{name} as ", "", ident + 4);

        handle_any_json_element(ident + 4, _type);

        if pos != field_list.len() - 1 {
            println!(",")
        } else {
            println!()
        }
    }

    print!("{: <1$})", "", ident);
}

fn handle_avro_array_type(ident: usize, object: &Map<String, Value>) {
    let items = object.get("items")
        .expect(format!("`array` type expect to have 'items' attribute, json = {:?}", object).as_str());

    handle_any_json_element(ident, items);
    print!("[]")
}

fn handle_avro_bytes_type(object: &Map<String, Value>) {
    let logical_type = object.get("logicalType")
        .expect(format!("`bytes` type expected to have 'logicalType' attribute, json = {:?}", object).as_str())
        .as_str()
        .expect(format!("`logicalType` field element expected to be of json string type, json = {:?}", object).as_str());

    match logical_type {
        "decimal" => handle_decimal_avro_type(object),
        _ => panic!("Unsupported `logicalType` {} for avro type `bytes`, json = {:?}", logical_type, object),
    }
}

fn handle_decimal_avro_type(object: &Map<String, Value>) {
    let precision = object.get("precision")
        .expect(format!("`decimal` logicalType expected to have 'precision' attribute, json = {:?}", object).as_str())
        .as_i64()
        .expect(format!("`precision` expected to be of numeric type, json = {:?}", object).as_str());

    let scale = object.get("scale")
        .expect(format!("`decimal` logicalType expected to have 'scale' attribute, json = {:?}", object).as_str())
        .as_i64()
        .expect(format!("`scale` expected to be of numeric type, json = {:?}", object).as_str());

    print!("decimal({},{})", precision, scale)
}


fn convert_avro_2_adf_schema(avro: &str) {
    let root: Value = serde_json::from_str(avro).unwrap();

    handle_any_json_element(0, &root)
}

fn main() -> io::Result<()> {
    let json_input = io::read_to_string(io::stdin())?;

    convert_avro_2_adf_schema(json_input.as_str());

    Ok(())
}
