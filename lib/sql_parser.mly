%{
    open Sql_types
%}

%token ASC
%token COLLATE
%token COMMA
%token CONCURRENTLY
%token CREATE
%token DESC
%token DOT
%token END
%token EXISTS
%token FIRST
%token IF
%token INCLUDE
%token INDEX
%token LAST
%token LPAREN
%token NOT
%token NULLS
%token ON
%token ONLY
%token RPAREN
%token TABLESPACE
%token UNIQUE
%token USING
%token WHERE
%token WITH
%token RELOP_LT
%token RELOP_LTE
%token RELOP_GT
%token RELOP_GTE
%token EQUAL
%token RELOP_NE
%token <string> BINOP
%token <Sql_types.value> VALUE
%token <string> IDENT

%start <Sql_types.statement> statement
%%

(* https://www.postgresql.org/docs/12/sql-createindex.html *)
statement:
| CREATE; unique = boption(UNIQUE); INDEX;
  boption(if_not_exists);
  boption(CONCURRENTLY);
  index = IDENT;
  ON; boption(ONLY); table = qual_ident;
  using = option(using); LPAREN; exprs = expressions_or_columns; RPAREN;
  option(include_);
  option(with_);
  option(tablespace);
  where = option(where);
  END
  {
    CreateIndex {
        index;
        unique;
        table;
        algorithm = using;
        fields = exprs;
        where;
      }
  }

include_:
| INCLUDE; ids = separated_nonempty_list(COMMA, qual_ident) { ids }

with_:
| WITH; ids = separated_nonempty_list(COMMA, with_setting) { ids }

with_setting:
| ident = IDENT; value = value { (ident, value) }

value:
| value = VALUE { value }
| value = IDENT { Sql_types.V_String value }
                           
where:
| WHERE; expr = expression { expr }
                           
tablespace:
| TABLESPACE; tablespace = IDENT { tablespace }
                           
expressions_or_columns:
| ids = separated_nonempty_list(COMMA, expression_or_column_with_collate) { ids }

expression_or_column_with_collate:
| expr = expression_or_column option(collate); option(asc_desc); option(nulls) { expr }

expression_or_column:
| id = IDENT { `Column id }
| LPAREN; expr = expression; RPAREN { `Expression expr }

expression:
| expr = IDENT { E_Identifier expr }
| value = VALUE { E_Literal value }
| func = IDENT; LPAREN; args = separated_list(COMMA, expression); RPAREN
  { E_FunCall (func, args) }
(* 
the next causes:

Warning: one state has shift/reduce conflicts.
Warning: 6 shift/reduce conflicts were arbitrarily resolved.
*)
| a = expression; op = binop; b = expression;
  { E_RelOp (a, op, b) }

qual_ident:
| ident = IDENT { ident }
| schema = IDENT; DOT; ident = IDENT { schema ^ "." ^ ident }

binop:
| RELOP_LT { "<" }
| RELOP_LTE { "<=" }
| RELOP_GT { ">" }
| RELOP_GTE { ">=" }
| EQUAL { "=" }
| RELOP_NE { "<>" }
  
asc_desc:
| ASC { `Asc }
| DESC { `Desc }

nulls:
| NULLS FIRST { `First }
| NULLS LAST { `LAst }
                                   
if_not_exists:
| IF NOT EXISTS { () }

collate:
| COLLATE collation = IDENT { collation }

using:
| USING; algorithm = IDENT { algorithm }
