package schema

import (
	"database/sql"
	"fmt"
	"io"
)

// GetSchema generate the code for given table and store it in w
func GetSchema(w io.Writer, db *sql.DB, tableName, dbName string) (string, error) {
	cols, err := readTableStruct(db, tableName, dbName)
	if nil != err {
		return "", err
	}
	if len(cols) == 0 {
		err = fmt.Errorf("Information not found, Check your params?")
		return "", err
	}
	r, structName, err := createStructSourceCode(cols, tableName)
	if nil != err {
		return "", err
	}
	_, err = io.Copy(w, r)
	return structName, err
}
