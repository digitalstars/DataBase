<?php


namespace DigitalStars\DataBase;


trait Parser {
    private $args;
    private $original_query;
    private $result_query;
    private $is_inner = false;

    public function parse($query, array $args) {
        $this->is_inner = false;
        $this->original_query = $query;
        $this->args = $args;
        $anon = function ($v) {
            return $this->parse_part($v);
        };
        $this->result_query = preg_replace_callback("!(\?[avw]\[.*?\])|(\?[avw]?[sidnf])!i", $anon, $query);
        return $this->result_query;
    }

    public function getQueryString() {
        return $this->result_query;
    }

    private function parse_part($symbol, $value = null) {
        if (is_array($symbol))
            $symbol = $symbol[0];
        if ($symbol == '?')
            return $symbol;
        $symbol = trim($symbol);
        if (!$this->is_inner and $symbol != '?n')
            if (count($this->args) == 0)
                throw new Exception('Несовпадение количества аргументов и заполнителей в массиве, запрос ' . $this->original_query);
            else
                $value = array_shift($this->args);
        switch ($symbol[1]) {
            case "S":
                $is_like_escaping = true;
            case 's':
                $value = $this->getValueStringType($value);
                return isset($is_like_escaping) ? $this->escapeLike($value) : $this->realEscapeString($value);
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
                    return join(',', $result);
                } else
                    return join(',', $value);
            case "v":
                if (!is_array($value))
                    throw new Exception($this->createErrorMessage('array', $value));
                foreach ($value as $key => $val)
                    $value[$key] = join(',', $this->parseArray($symbol, $value[$key]));
                return "(".join("),(", $value).")";
            case "w":
                $value = $this->parseArray($symbol, $value);
                foreach ($value as $key => $val)
                    $result[] = $this->escapeFieldName($key) . "=" . $val;
                return join(' AND ', $result);
            default:
                throw new Exception("Неизвестный заполнитель {$symbol[2]}");

        }
    }

    private function parseArray($symbol, $value) {
        if (!is_array($value))
            throw new Exception($this->createErrorMessage('array', $value));
        if ($symbol[2] == 'i')
            foreach ($value as $key => $val)
                $value[$key] = $this->getValueIntType($val);
        else if ($symbol[2] == 'd')
            foreach ($value as $key => $val)
                $value[$key] = $this->getValueFloatType($val);
        else if ($symbol[2] == 's')
            foreach ($value as $key => $val)
                $value[$key] = $this->realEscapeString($this->getValueStringType($val));
        else if ($symbol[2] == 'f')
            foreach ($value as $key => $val)
                $value[$key] = $this->escapeFieldName($val);
        else if ($symbol[2] == '[') {
            $selectors = explode(',',
                substr($symbol, 3, strlen($symbol) - 4));
            if (count($selectors) != count($value))
                throw new Exception('Несовпадение количества аргументов и заполнителей в массиве, запрос ' . $this->original_query);
            $this->is_inner = true;
            $index = 0;
            foreach ($value as $key => $val)
                $value[$key] = $this->parse_part($selectors[$index++], $val);
            $this->is_inner = false;
        } else
            throw new Exception("Неизвестный заполнитель {$symbol[2]}");
        return $value;
    }

    private function escapeFieldName($value) {
        if (!is_string($value)) {
            throw new Exception($this->createErrorMessage('field', $value));
        }

        $new_value = '';

        $replace = function ($value) {
            return '`' . str_replace("`", "``", $value) . '`';
        };

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
        } else {
            return $replace($value);
        }
    }

    private function getValueIntType($value) {
        if (is_integer($value))
            return $value;
        if (is_numeric($value) || is_null($value) || is_bool($value)) {
            return (int)$value;
        }
        throw new Exception($this->createErrorMessage('integer', $value));
    }

    private function getValueFloatType($value) {
        if (is_float($value)) {
            return $value;
        }
        if (is_numeric($value) || is_null($value) || is_bool($value)) {
            return (float)$value;
        }
        throw new Exception($this->createErrorMessage('double', $value));
    }

    private function getValueStringType($value) {
        // меняем поведение PHP в отношении приведения bool к string
        if (is_bool($value)) {
            return (string)(int)$value;
        }

        if (!is_string($value) && !(is_numeric($value) || is_null($value))) {
            throw new Exception($this->createErrorMessage('string', $value));
        }

        return (string)$value;
    }

    private function escapeLike($var, $chars = "%_") {
        $var = str_replace('\\', '\\\\', $var);
        $var = $this->realEscapeString($var);

        if ($chars) {
            $var = addCslashes($var, $chars);
        }

        return $var;
    }

    private function realEscapeString($value) {
        return $this->quote($value);
    }

    private function createErrorMessage($type, $value) {
        return "Попытка указать для заполнителя типа $type значение типа " . gettype($value) . " в шаблоне запроса $this->original_query";
    }
}