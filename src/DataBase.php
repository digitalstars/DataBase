<?php

namespace DigitalStars\DataBase;

use PDO;

class DataBase extends \PDO {
    use Parser;

    public function __construct($dsn, $username = null, $passwd = null, $options = null) {
        parent::__construct($dsn, $username, $passwd, $options);
    }

    public function exec($statement, $args = []) {
        return parent::exec(count($args) == 0 ? $statement : $this->parse($statement, $args));
    }

    public function query($statement, $args = [], $mode = null, $arg3 = null, $ctorargs = null) {
        $statement = (count($args) == 0 ? $statement : $this->parse($statement, $args));
        if (!is_null($ctorargs))
            return parent::query($statement, $mode, $arg3, $ctorargs);
        else if (!is_null($arg3))
            return parent::query($statement, $mode, $arg3);
        else if (!is_null($mode))
            return parent::query($statement, $mode);
        return parent::query($statement);
    }

    public function prepare($statement, $args = [], array $driver_options = array()) {
        return parent::prepare(count($args) == 0 ? $statement : $this->parse($statement, $args), $driver_options);
    }

    public function rows($sql, $args = [], $fetchMode = PDO::FETCH_OBJ) {
        return $this->query($sql, $args)->fetchAll($fetchMode);
    }

    public function row($sql, $args = [], $fetchMode = PDO::FETCH_OBJ) {
        return $this->query($sql, $args)->fetch($fetchMode);
    }

    public function getById($table, $id, $fetchMode = PDO::FETCH_OBJ) {
        return $this->query("SELECT * FROM ?f WHERE id = ?i", [$table, $id])->fetch($fetchMode);
    }

    public function count($sql, $args = []) {
        return $this->query($sql, $args)->rowCount();
    }

    public function insert($table, $data) {
        $this->query("INSERT INTO ?f (?af) VALUES (?as)", [$table, array_keys($data), array_values($data)]);
        return $this->lastInsertId();
    }

    public function update($table, $data, $where) {
        return $this->exec("UPDATE ?f SET ?As WHERE ?ws", [$table, $data, $where]);
    }

    public function delete($table, $where, $limit = 1) {
        return $this->exec("DELETE FROM ?f WHERE ?ws LIMIT ?i", [$table, $where, $limit]);
    }

    public function deleteAll($table) {
        return $this->exec("DELETE FROM ?f", [$table]);
    }

    public function deleteById($table, $id) {
        return $this->exec("DELETE FROM ?f WHERE id = ?i", [$table, $id]);
    }

    public function deleteByIds($table, $column, $ids) {
        return $this->exec("DELETE FROM ?f WHERE ?f IN (?as)", [$table, $column, $ids]);
    }

    public function truncate($table) {
        return $this->exec("TRUNCATE TABLE ?f", [$table]);
    }
}