open OUnit2

module Lexer = Influxdb_write_to_postgresql.Influxdb_lexer

let flip f a b = f b a

let testEmpty _ctx =
  let open Lexer in
  flip assert_raises (fun () ->
      line_exn (Sedlexing.Utf8.from_string {||})
    ) (Error {info=Parse_error; message="Expected measurement name (table)"})

let testOnlyMeasurement _ctx =
  let open Lexer in
  flip assert_raises (fun () ->
      line_exn (Sedlexing.Utf8.from_string {|meas|})
    )
    (Error {info=Parse_error; message="Expected fields"})

let testOnlyMeasurementTime _ctx =
  let open Lexer in
  flip assert_raises (fun () ->
      line_exn (Sedlexing.Utf8.from_string {|meas 1234|})
    )
    (* TODO: what? why not comma? *)
    (Error {info=Parse_error; message="Expected equal sign; received <>"})

let testOnlyTag _ctx =
  let open Lexer in
  flip assert_raises (fun () ->
      line_exn (Sedlexing.Utf8.from_string {|meas,tag=42 1234|})
    )
    (* TODO: what? why not fields? *)
    (Error {info=Parse_error; message="Expected equal sign; received <>"})

let testMissingMeasurement1 _ctx =
  let open Lexer in
  flip assert_raises (fun () ->
      line_exn (Sedlexing.Utf8.from_string {|id=42 field=42i 1234|})
    )
    (* TODO: why not measurement? *)
    (Error {info=Parse_error; message="Expected fields"})

let testMissingMeasurement2 _ctx =
  let open Lexer in
  flip assert_raises (fun () ->
      line_exn (Sedlexing.Utf8.from_string {|,id=42 field=42i 1234|})
    )
    (Error {info=Parse_error; message="Expected measurement name (table)"})

let testTypes _ctx =
  let open Lexer in
  let meas = line_exn (Sedlexing.Utf8.from_string {|meas integer=1i,float1=1,float2=1.,float3=1.0,float4=1.00,bool1=f,bool2=F,bool3=false,bool4=False,bool5=FALSE,bool6=t,bool7=T,bool8=true,bool9=True,bool10=TRUE,string1="",string2=" ",string3="\""|}) in
  flip assert_equal meas.measurement "meas";
  flip assert_equal meas.tags [];
  assert_equal ~printer:[%derive.show: (string * value) list] [
    ("integer", Int 1L);
    ("float1", FloatNum 1.0);
    ("float2", FloatNum 1.0);
    ("float3", FloatNum 1.0);
    ("float4", FloatNum 1.0);
    ("bool1", Boolean false);
    ("bool2", Boolean false);
    ("bool3", Boolean false);
    ("bool4", Boolean false);
    ("bool5", Boolean false);
    ("bool6", Boolean true);
    ("bool7", Boolean true);
    ("bool8", Boolean true);
    ("bool9", Boolean true);
    ("bool10", Boolean true);
    ("string1", String "");
    ("string2", String " ");
    ("string3", String "\"");
  ] meas.fields;
  flip assert_equal meas.time None

let testOnlyFieldInt _ctx =
  let open Lexer in
  let meas = line_exn (Sedlexing.Utf8.from_string {|meas field=42i 1234|}) in
  flip assert_equal meas.measurement "meas";
  flip assert_equal meas.tags [];
  flip assert_equal meas.fields [("field", Int 42L)];
  flip assert_equal meas.time (Some 1234L)

let testOnlyFieldFloat _ctx =
  let open Lexer in
  let meas = 
    line_exn (Sedlexing.Utf8.from_string {|meas field=42.0 1234|})
  in
  flip assert_equal meas.measurement "meas";
  flip assert_equal meas.tags [];
  flip assert_equal meas.fields [("field", FloatNum 42.0)];
  flip assert_equal meas.time (Some 1234L)

let testOnlyFieldString _ctx =
  let open Lexer in
  let meas = 
    line_exn (Sedlexing.Utf8.from_string {|meas field="moi" 1234|})
  in
  flip assert_equal meas.measurement "meas";
  flip assert_equal meas.tags [];
  flip assert_equal meas.fields [("field", String "moi")];
  flip assert_equal meas.time (Some 1234L)

let testTagField _ctx =
  let open Lexer in
  let meas = 
    line_exn (Sedlexing.Utf8.from_string {|meas,id=55 field=42i 1234|})
  in
  flip assert_equal meas.measurement "meas";
  flip assert_equal meas.tags [("id", "55")];
  flip assert_equal meas.fields [("field", Int 42L)];
  flip assert_equal meas.time (Some 1234L)


let testTagFieldNoTime _ctx =
  let open Lexer in
  let meas = 
    line_exn (Sedlexing.Utf8.from_string {|meas,id=55 field=42i|})
  in
  flip assert_equal meas.measurement "meas";
  flip assert_equal meas.tags [("id", "55")];
  flip assert_equal meas.fields [("field", Int 42L)];
  flip assert_equal meas.time None

let testTagFields _ctx =
  let open Lexer in
  let meas = 
    line_exn (Sedlexing.Utf8.from_string {|meas,id=55 field="moi",field2=44i 1234|})
  in
  flip assert_equal meas.measurement "meas";
  flip assert_equal meas.tags [("id", "55")];
  flip assert_equal meas.fields [("field", String "moi"); ("field2", Int 44L)];
  flip assert_equal meas.time (Some 1234L)

let testTagsFields _ctx =
  let open Lexer in
  let meas = 
    line_exn (Sedlexing.Utf8.from_string {|meas,id=55,borf="plop" field="moi",field2=44i 1234|})
  in
  flip assert_equal meas.measurement "meas";
  flip assert_equal meas.tags [("id", "55"); ("borf", {|"plop"|})];
  flip assert_equal meas.fields [("field", String "moi"); ("field2", Int 44L)];
  flip assert_equal meas.time (Some 1234L)

let testTagQuoting _ctx =
  let open Lexer in
  let meas = 
    line_exn (Sedlexing.Utf8.from_string {|meas,id="55\,42\\\=" field="moi",field2=44i 1234|})
  in
  flip assert_equal meas.measurement "meas";
  flip assert_equal meas.tags [("id", {|"55,42\="|})];
  flip assert_equal meas.fields [("field", String "moi"); ("field2", Int 44L)];
  flip assert_equal meas.time (Some 1234L)

let testValueQuoting _ctx =
  let open Lexer in
  let meas = 
    line_exn (Sedlexing.Utf8.from_string {|meas,id=1 field="moi\"taas" 1234|})
  in
  flip assert_equal meas.measurement "meas";
  flip assert_equal meas.tags [("id", {|1|})];
  flip assert_equal meas.fields [("field", String {|moi"taas|})];
  flip assert_equal meas.time (Some 1234L)

let testTwo _ctx =
  let open Lexer in
  let meas1, meas2 =
    match
      lines_exn (Sedlexing.Utf8.from_string {|meas,id=1 field="moi\"taas" 1234
meas,id=2 field="moi2" 1235|})
    with
    | meas1::meas2::[] -> (meas1, meas2)
    | _ -> assert_failure "Invalid number of results from lines"
  in
  flip assert_equal meas1.measurement "meas";
  flip assert_equal meas1.tags [("id", {|1|})];
  flip assert_equal meas1.fields [("field", String {|moi"taas|})];
  flip assert_equal meas1.time (Some 1234L);
  flip assert_equal meas2.measurement "meas";
  flip assert_equal meas2.tags [("id", {|2|})];
  flip assert_equal meas2.fields [("field", String {|moi2|})];
  flip assert_equal meas2.time (Some 1235L)

let testTwoNoTime _ctx =
  let open Lexer in
  let meas1, meas2 =
    match
      lines_exn (Sedlexing.Utf8.from_string {|meas,id=1 field="moi\"taas"
meas,id=2 field="moi2"
|})
    with
    | meas1::meas2::[] -> (meas1, meas2)
    | _ -> assert_failure "Invalid number of results from lines"
  in
  flip assert_equal meas1.measurement "meas";
  flip assert_equal meas1.tags [("id", {|1|})];
  flip assert_equal meas1.fields [("field", String {|moi"taas|})];
  flip assert_equal meas1.time None;
  flip assert_equal meas2.measurement "meas";
  flip assert_equal meas2.tags [("id", {|2|})];
  flip assert_equal meas2.fields [("field", String {|moi2|})];
  flip assert_equal meas2.time None


let suite = "Infludb_writer_lexer" >::: [
    "testEmpty" >:: testEmpty;
    "testOnlyMeasurement" >:: testOnlyMeasurement;
    "testOnlyMeasurementTime" >:: testOnlyMeasurementTime;
    "testOnlyTag" >:: testOnlyTag;
    "testTypes" >:: testTypes;
    "testMissingMeasurement1" >:: testMissingMeasurement1;
    "testMissingMeasurement2" >:: testMissingMeasurement2;
    "testOnlyFieldInt" >:: testOnlyFieldInt;
    "testOnlyFieldFloat" >:: testOnlyFieldFloat;
    "testOnlyFieldString" >:: testOnlyFieldString;
    "testTagField" >:: testTagField;
    "testTagFieldNoTime" >:: testTagFieldNoTime;
    "testTagFields" >:: testTagFields;
    "testTagsFields" >:: testTagsFields;
    "testTagQuoting" >:: testTagQuoting;
    "testValueQuoting" >:: testValueQuoting;
    "testTwo" >:: testTwo;
    "testTwoNoTime" >:: testTwoNoTime;
  ]
