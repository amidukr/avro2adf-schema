extern crate core;

use std::io;
use serde_json::value::Value;
use serde_json::Map;

trait ValueExt {
    fn as_array(&self) -> Option<&Vec<Value>>;
    fn as_object_value(&self) -> Option<&Map<String, Value>>;
    fn as_string(&self) -> Option<&String>;
}

impl ValueExt for Value {
    fn as_array(&self) -> Option<&Vec<Value>> {
        return  match self {
            Value::Array(o) => Some(o),
            _ => None
        }
    }

    fn as_object_value(&self) -> Option<&Map<String, Value>> {
        return  match self {
            Value::Object(o) => Some(o),
            _ => None
        }
    }

    fn as_string(&self) -> Option<&String> {
        return  match self {
            Value::String(o) => Some(o),
            _ => None
        }
    }
}

fn handle_type(ident: usize, json: &Value) {
    match json {
        Value::Null => {}
        Value::String(o) => handle_string_type(o),
        Value::Array(o) => handle_array_type(ident, o),
        Value::Object(o) => handle_map_type(ident, o),
        _ => panic!("Unexpected avro type tag: {}", json),
    }
}

fn handle_map_type(ident: usize, object: &Map<String, Value>) {

    if let Some(Value::String(t)) = object.get("type") {
        match t.as_str() {
            "record" => {handle_record_type(ident, object); return;},
            _ => {}
        }
    }

    panic!("Unexpected type definition = {:?}", object);
}

fn handle_record_type(ident: usize, object: &Map<String, Value>) {
    let fields = object.get("fields")
        .expect(format!("`record` type expect to have 'fields' attribute, json = {:?}", object).as_str());

    let field_list = fields.as_array()
        .expect(format!("`fields` attribute expected to be of json array type, fields = {}", fields).as_str());

    
    println!("{: <1$}(", "", ident);

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

        handle_type(ident + 4, _type);

        if pos != field_list.len() - 1 {
            println!(",")
        }else{
            println!()
        }
    }

    println!("{: <1$})", "", ident);
}

fn handle_array_type(ident: usize, array: &Vec<Value>) {
    let type_array:Vec<_> = array.iter()
        .filter(|t| match t {
            Value::String(s) if s != "null" => true,
            _ => true
        }
        ).collect();

    if type_array.len() != 1 {
        panic!("Array of types has different to 1 of non-nullable types, {:?}", array);
    }

    handle_type(ident, type_array.into_iter().next()
        .unwrap())
}

fn handle_string_type(t: &String)  {
    print!("{}", t)
}

fn convert_avro_2_adf_schema(avro: &str)  {
    let root: Value = serde_json::from_str(avro).unwrap();

    handle_type(0, &root)
}

fn main() -> io::Result<()>{
    let json_input = io::read_to_string(io::stdin())?;

    convert_avro_2_adf_schema(json_input.as_str());

    Ok(())
}
