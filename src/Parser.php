<?php


namespace DigitalStars\DataBase;


trait Parser {
    private $args;
    
    private string $original_query;
    
    private $result_query;
    
    private bool $is_inner = false;

    public function parse(string $query, array $args): string|array|null {
        $this->is_inner = false;
        $this->original_query = $query;
        $this->args = $args;
        $anon = fn($v) => $this->parse_part($v);
        $this->result_query = preg_replace_callback("!(\?[avw]\[.*?\])|(\?[avw]?[sidnf])!i", $anon, $query);
        return $this->result_query;
    }

    public function getQueryString() {
        return $this->result_query;
    }

    private function parse_part($symbol, $value = null) {
        if (is_array($symbol)) {
            $symbol = $symbol[0];
        }
        
        if ($symbol == '?') {
            return $symbol;
        }
        
        $symbol = trim($symbol);
        if (!$this->is_inner && $symbol != '?n') {
            if (count($this->args) == 0) {
                throw new Exception('Несовпадение количества аргументов и заполнителей в массиве, запрос ' . $this->original_query);
            }
            $value = array_shift($this->args);
        }
        
        switch ($symbol[1]) {
            case "S":
                $is_like_escaping = true;
            case 's':
                return $this->getValueStringType($value, isset($is_like_escaping));
            case 'i':
                return $this->getValueIntType($value);
            case 'd':
                return $this->getValueFloatType($value);
            case 'n':
                return "NULL";
            case 'f':
                return $this->escapeFieldName($value);
            case 'A':
                $is_associative_array = true;
            case 'a':
                $value = $this->parseArray($symbol, $value);
                if (isset($is_associative_array)) {
                    foreach ($value as $key => $val)
                        $result[] = $this->escapeFieldName($key) . "=" . $val;
                    
                    return implode(',', $result);
                }
                return implode(',', $value);
            case "v":
                if (!is_array($value)) {
                    throw new Exception($this->createErrorMessage('array', $value));
                }
                
                foreach (array_keys($value) as $key)
                    $value[$key] = implode(',', $this->parseArray($symbol, $value[$key]));
                
                return "(".implode("),(", $value).")";
            case "w":
                $value = $this->parseArray($symbol, $value);
                foreach ($value as $key => $val)
                    $result[] = $this->escapeFieldName($key) . "=" . $val;
                
                return implode(' AND ', $result);
            default:
                throw new Exception('Неизвестный заполнитель ' . $symbol[2]);

        }
    }

    private function parseArray($symbol, $value): array {
        if (!is_array($value)) {
            throw new Exception($this->createErrorMessage('array', $value));
        }
        
        if ($symbol[2] == 'i') {
            foreach ($value as $key => $val)
                $value[$key] = $this->getValueIntType($val);
        } elseif ($symbol[2] == 'd') {
            foreach ($value as $key => $val)
                $value[$key] = $this->getValueFloatType($val);
        } elseif ($symbol[2] == 's') {
            foreach ($value as $key => $val)
                $value[$key] = $this->getValueStringType($val);
        } elseif ($symbol[2] == 'f') {
            foreach ($value as $key => $val)
                $value[$key] = $this->escapeFieldName($val);
        } elseif ($symbol[2] == '[') {
            $selectors = explode(',',
                substr($symbol, 3, strlen($symbol) - 4));
            if (count($selectors) !== count($value)) {
                throw new Exception('Несовпадение количества аргументов и заполнителей в массиве, запрос ' . $this->original_query);
            }

            $this->is_inner = true;
            $index = 0;
            foreach ($value as $key => $val)
                $value[$key] = $this->parse_part($selectors[$index++], $val);

            $this->is_inner = false;
        } else {
            throw new Exception('Неизвестный заполнитель ' . $symbol[2]);
        }
        
        return $value;
    }

    private function escapeFieldName($value): string {
        if (!is_string($value)) {
            throw new Exception($this->createErrorMessage('field', $value));
        }

        $new_value = '';

        $replace = static fn($value): string => '`' . str_replace("`", "``", $value) . '`';

        $dot = false;

        if ($values = explode('.', $value)) {
            foreach ($values as $value) {
                if ($value === '') {
                    if (!$dot) {
                        $dot = true;
                        $new_value .= '.';
                    } else {
                        throw new Exception('Два символа `.` идущие подряд в имени столбца или таблицы');
                    }
                } else {
                    $new_value .= $replace($value) . '.';
                }
            }

            return rtrim($new_value, '.');
        }
        return $replace($value);
    }

    private function getValueIntType($value): int|string {
        if (is_int($value)) {
            return $value;
        }
        
        if (is_numeric($value) || is_bool($value)) {
            return (int)$value;
        }
        
        if (is_null($value)) {
            return 'null';
        }
        
        throw new Exception($this->createErrorMessage('integer', $value));
    }

    private function getValueFloatType($value): float|string {
        if (is_float($value)) {
            return $value;
        }
        
        if (is_numeric($value) || is_bool($value)) {
            return (float)$value;
        }
        
        if (is_null($value)) {
            return 'null';
        }
        
        throw new Exception($this->createErrorMessage('double', $value));
    }

    private function getValueStringType($value, $is_like_escaping = false) {
        // меняем поведение PHP в отношении приведения bool к string
        if (is_bool($value)) {
            $value = (string)(int)$value;
        }
        
        if (is_null($value)) {
            return 'null';
        }
        
        if (!is_scalar($value)) {
            throw new Exception($this->createErrorMessage('string', $value));
        }

        return $is_like_escaping ? $this->escapeLike($value) : $this->realEscapeString($value);
    }

    private function escapeLike($var, $chars = "%_") {
        $var = str_replace('\\', '\\\\', $var);
        $var = $this->realEscapeString($var);

        if ($chars) {
            return addCslashes($var, $chars);
        }

        return $var;
    }

    private function realEscapeString($value) {
        return $this->pdo->quote($value);
    }

    private function createErrorMessage($type, $value): string {
        return sprintf('Попытка указать для заполнителя типа %s значение типа ', $type) . gettype($value) . (' в шаблоне запроса ' . $this->original_query);
    }
}