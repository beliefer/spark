### Evaluation Functions
## Summary
  - Number of functions: 3
## Conditional functions
| No | Function Name | Usage | Arguments | Examples | Since | Note | Deprecated |
| -- | ------------- | ----- | --------- | -------- | ----- | ---- | ---------- |
| 0 | case | CASE( expr1, expr2 [, expr3, expr4]*)<br>- When `expr1` = true, returns `expr2`; else when `expr3` = true, returns `expr4`. | Arguments:<br>* expr1, expr3 - the branch condition expressions should all be boolean type.<br>* expr2, expr4, expr5 - the branch value expressions should all be<br>same type or coercible to a common type. | Examples:<br>> EVAL x = CASE( 1 > 0, 1, 2 > 0, 2.0)<br>x = 1<br>> EVAL y = CASE( 1 < 0, 1, 2 > 0, 2.0)<br>y = 2.0<br>> EVAL z = CASE( 1 < 0, 1, 2 < 0, 2.0)<br>z = NULL | 1.0.0 |  |  |
## Conversion functions
| No | Function Name | Usage | Arguments | Examples | Since | Note | Deprecated |
| -- | ------------- | ----- | --------- | -------- | ----- | ---- | ---------- |
| 1 | tonumber | ToNumber(expr1) Returns the number converted from the input string. |  | Examples:<br>> EVAL x = ToNumber("101101", 2)<br>  x = 45<br>> EVAL y = ToNumber("10437", 8)<br>  y = 4383<br>> EVAL z = ToNumber("12.25")<br>  z = 12.25<br>> EVAL a = ToNumber("12.25", 10)<br>  a = 12.25<br>> EVAL b = ToNumber("0A4", 16)<br>  b = 164 | 1.0.0 |  |  |
## String functions
| No | Function Name | Usage | Arguments | Examples | Since | Note | Deprecated |
| -- | ------------- | ----- | --------- | -------- | ----- | ---- | ---------- |
| 2 | lower | LOWER(str) - Returns `str` with all characters changed to lowercase. |  | Examples:<br/>> EVAL x = LOWER('Qcloud Analytics')<br/>x = "qcloud analytics" | 1.0.0 |  |  |