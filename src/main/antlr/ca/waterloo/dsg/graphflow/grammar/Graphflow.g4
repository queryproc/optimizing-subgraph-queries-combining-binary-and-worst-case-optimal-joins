grammar Graphflow;

graphflow : whitespace? matchPattern whitespace? (LIMIT whitespace Digits whitespace?)? (SEMICOLON whitespace?)? EOF ;

matchPattern : edge ( whitespace? COMMA whitespace? edge )* ;
edge   : vertex whitespace? DASH label? GREATER_THAN vertex ;
vertex : OPEN_ROUND_BRACKET whitespace? variable (type)? whitespace? CLOSE_ROUND_BRACKET ;
type   : whitespace? COLON whitespace? variable ;
label  : OPEN_SQUARE_BRACKET variable CLOSE_SQUARE_BRACKET DASH;
variable   : ( Digits | Characters | UNDERSCORE ) ( Digits | Characters | UNDERSCORE )* ;
whitespace : ( SPACE | TAB | CARRIAGE_RETURN | LINE_FEED | FORM_FEED | Comment )+ ;

/*********** Lexer rules ***********/

LIMIT : L I M I T ;

fragment EscapedChar : TAB | CARRIAGE_RETURN | LINE_FEED | BACKSPACE | FORM_FEED | '\\' ( '"' | '\'' | '\\' ) ;

QuotedCharacter : SINGLE_QUOTE ( EscapedChar | ~( '\\' | '\'' ) ) SINGLE_QUOTE ;
QuotedString : DOUBLE_QUOTE ( EscapedChar | ~( '"' ) )* DOUBLE_QUOTE
             | SINGLE_QUOTE ( EscapedChar | ~( '\'' ) )* SINGLE_QUOTE ;

Comment : '/*' .*? '*/'
        | '//' ~( '\n' | '\r' )*  '\r'? ( '\n' | EOF ) ;

SPACE : [ ]  ;
TAB   : [\t] ;
LINE_FEED : [\n] ;
FORM_FEED : [\f] ;
BACKSPACE : [\b] ;
VERTICAL_TAB : [\u000B] ;
CARRIAGE_RETURN : [\r]  ;

DASH : '-' ;
UNDERSCORE : '_' ;
FORWARD_SLASH  : '/'  ;
BACKWARD_SLASH : '\\' ;
SEMICOLON: ';' ;
COMMA : ',' ;
COLON : ':' ;
SINGLE_QUOTE : '\'' ;
DOUBLE_QUOTE : '"'  ;
OPEN_ROUND_BRACKET   : '(' ;
CLOSE_ROUND_BRACKET  : ')' ;
OPEN_SQUARE_BRACKET  : '[' ;
CLOSE_SQUARE_BRACKET : ']' ;
GREATER_THAN : '>' ;

Digits : ( Digit )+ ;
Characters : ( Character )+ ;

fragment Digit : '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' ;

fragment Character : A | B | C | D | E | F | G | H | I | J | K | L | M |
                     N | O | P | Q | R | S | T | U | V | W | X | Y | Z ;
fragment A : ('a'|'A') ;
fragment B : ('b'|'B') ;
fragment C : ('c'|'C') ;
fragment D : ('d'|'D') ;
fragment E : ('e'|'E') ;
fragment F : ('f'|'F') ;
fragment G : ('g'|'G') ;
fragment H : ('h'|'H') ;
fragment I : ('i'|'I') ;
fragment J : ('j'|'J') ;
fragment K : ('k'|'K') ;
fragment L : ('l'|'L') ;
fragment M : ('m'|'M') ;
fragment N : ('n'|'N') ;
fragment O : ('o'|'O') ;
fragment P : ('p'|'P') ;
fragment Q : ('q'|'Q') ;
fragment R : ('r'|'R') ;
fragment S : ('s'|'S') ;
fragment T : ('t'|'T') ;
fragment U : ('u'|'U') ;
fragment V : ('v'|'V') ;
fragment W : ('w'|'W') ;
fragment X : ('x'|'X') ;
fragment Y : ('y'|'Y') ;
fragment Z : ('z'|'Z') ;
