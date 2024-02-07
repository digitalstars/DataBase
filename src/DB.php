<?php

namespace DigitalStars\DataBase;

use PDO;
use PDOException;

class DB {
    use Parser;

    private $anon_log_func;

    private array $options;

    public PDO $pdo;
    
    public array $error_codes_to_repeat = [1040, 1159, 1160, 1161, 2002, 2003, 2006, 1213]; //1158
    
    // 1040 - Too many connections
    // 1158 - [Network error] Got a packet bigger than 'max_allowed_packet' bytes
    // 1159 - [Network error] Got timeout reading communication packets
    // 1160 - [Network error] Got an error reading communication packets
    // 1161 - [Network error] Got packets out of order
    // 2002 - Connection refused
    // 2003 - Can't connect to MySQL server
    // 2006 - MySQL server has gone away
    // 1213 - Deadlock found when trying to get lock


    public function __construct(private string $dsn, private ?string $username = null, private ?string $passwd = null, ?array $options = null) {
        if (is_array($options)) {
            $options = array_replace([PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION], $options);
        }
        else {
            $options = [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION];
        }
        $this->options = $options;
        for ($i = 1; $i <= 6; ++$i) {
            try {
                $this->pdo = new PDO($this->dsn, $this->username, $this->passwd, $options);
            } catch (\PDOException $e) {

                if($i <= 5) {
                    $this->exceptionProcessing($e, $i);
                    continue;
                }

                throw new \PDOException($e, $e->getCode(), $e);
            }
        }
    }

    public function setLoggerFunc($func): void {
        $this->anon_log_func = $func;
    }

    public function pdo() {
        return $this->pdo;
    }

    public static function create($dsn, $username = null, $passwd = null, $options = null): DB {
        return new self($dsn, $username, $passwd, $options);
    }

    public function exec(string $statement, array $args = []) {
        for ($i = 1; $i <= 6; ++$i) {
            $statement = (count($args) === 0 ? $statement : $this->parse($statement, $args));
            try {
                $func = $this->anon_log_func;
                if($func) {
                    $func($statement . "\n-----------------");
                }
            } catch (Exception $e) {}
            
            try {
                return $this->pdo->exec($statement);
            } catch (\PDOException $e) {
                if($i <= 5) {
                    $this->exceptionProcessing($e, $i);
                    continue;
                }

                throw new \PDOException($e, $e->getCode(), $e);
            }
        }
    }

    public function execCommit(string $statement, array $args = []) {
        try {
            $this->pdo->beginTransaction(); // Начало транзакции
            $result = $this->exec($statement, $args);
            $this->pdo->commit(); // Фиксация изменений, если SQL-запрос выполнен успешно
            return $result;
        } catch (\PDOException $e) {
            $this->pdo->rollBack(); // Откат транзакции в случае ошибки
            throw $e; // Переброс исключения
        }
    }

    public function query(string $statement, array $args = [], $mode = null, $arg3 = null, $ctorargs = null) {
        $statement = (count($args) === 0 ? $statement : $this->parse($statement, $args));
        for ($i = 1; $i <= 6; ++$i) {
            try {
                $func = $this->anon_log_func;
                if($func) {
                    $func($statement . "\n-----------------");
                }
            } catch (Exception $e) {}
            
            try {
                if (!is_null($ctorargs)) {
                    $result = $this->pdo->query($statement, $mode, $arg3, $ctorargs);
                } elseif (!is_null($arg3)) {
                    $result = $this->pdo->query($statement, $mode, $arg3);
                } elseif (!is_null($mode)) {
                    $result = $this->pdo->query($statement, $mode);
                } else {
                    $result = $this->pdo->query($statement);
                }
                
                return $result;
            } catch (PDOException $e) {
                if($i <= 5) {
                    $this->exceptionProcessing($e, $i);
                    continue;
                }

                throw new PDOException($e, $e->getCode(), $e);
            }
        }
    }

    private function exceptionProcessing (\PDOException $e, int $i): void {
        $msg = $e->getMessage();
        if(!isset($e->errorInfo[1])) {
            trigger_error($msg."\nНет кода ошибки. Попытка перезапуска запроса [{$i}/5]", E_USER_WARNING);
            return;
        }
        
        if (in_array($e->errorInfo[1], $this->error_codes_to_repeat) || $e->errorInfo[0] == 40001) {
            if ($e->errorInfo[1] == 2006) {
                trigger_error($msg."\nПопытка пересоздания конструктора и запуска запроса [{$i}/5]", E_USER_WARNING);
                $this->__construct($this->dsn, $this->username, $this->passwd, $this->options);
            } else {
                trigger_error($msg."\nПопытка перезапуска запроса [{$i}/5]", E_USER_WARNING);
            }
        }
    }

    public function prepare(string $statement, array $args = [], array $driver_options = []) {
        return $this->pdo->prepare(count($args) === 0 ? $statement : $this->parse($statement, $args), $driver_options);
    }

    public function rows(string $sql, array $args = [], $fetchMode = PDO::FETCH_ASSOC) {
        $result = $this->query($sql, $args);
        $a = $result !== false ? $result->fetchAll($fetchMode) : false;
        $result->closeCursor();
        return $a;
    }

    public function row($sql, $args = [], $fetchMode = PDO::FETCH_ASSOC) {
        $result = $this->query($sql, $args);
        return $result !== false ? $result->fetch($fetchMode) : false;
    }

    public function count(string $sql, array $args = []) {
        $result = $this->query($sql, $args);
        return $result !== false ? $result->rowCount() : false;
    }

    public function getById($table, $id, $fetchMode = PDO::FETCH_ASSOC) {
        if (is_array($id)) {
            $result = $this->query("SELECT * FROM ?f WHERE ?ws", [$table, $id]);
        } else {
            $result = $this->query("SELECT * FROM ?f WHERE id = ?i", [$table, $id]);
        }
        
        return $result !== false ? $result->fetch($fetchMode) : false;
    }

    public function insert($table, $data) {
        $result = $this->query("INSERT INTO ?f (?af) VALUES (?as)", [$table, array_keys($data), array_values($data)]);
        return $result !== false ? $this->pdo->lastInsertId() : false;
    }

    public function update($table, $data, $where = []) {
        if (empty($where)) {
            return $this->execCommit("UPDATE ?f SET ?As WHERE 1", [$table, $data]);
        }
        return $this->execCommit("UPDATE ?f SET ?As WHERE ?ws", [$table, $data, $where]);
    }

    public function delete($table, $where, $limit = -1) {
        if ($limit === -1) {
            return $this->execCommit("DELETE FROM ?f WHERE ?ws", [$table, $where]);
        }
        return $this->execCommit("DELETE FROM ?f WHERE ?ws LIMIT ?i", [$table, $where, $limit]);
    }

    public function deleteAll($table) {
        return $this->execCommit("DELETE FROM ?f", [$table]);
    }

    public function deleteById($table, $id) {
        return $this->execCommit("DELETE FROM ?f WHERE id = ?i", [$table, $id]);
    }

    public function deleteByIds($table, $column, $ids) {
        return $this->execCommit("DELETE FROM ?f WHERE ?f IN (?as)", [$table, $column, $ids]);
    }

    public function truncate($table) {
        return $this->execCommit("TRUNCATE TABLE ?f", [$table]);
    }
}
