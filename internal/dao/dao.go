package dao

import (
	"bytes"
	"io"
	"text/template"
)

const (
	daoCode = `

	//Get{{.StructName}} gets one record from table {{.TableName}} by condition "where"
	func Get{{.StructName}}(db *gorm.DB, where map[string]interface{}, selectField []string) (*{{.StructName}}, error) {
		cond,vals,err := builder.BuildSelect("{{.TableName}}", where, selectField)
		if nil != err {
			return nil, err
		}
		row, err := db.Raw(cond, vals...).Rows()
		if nil != err || nil == row {
			return nil, err
		}
		defer row.Close()
		var res *{{.StructName}}
		err = scanner.Scan(row, &res)
		return res,err
	}

	//GetMulti{{.StructName}} gets multiple records from table {{.TableName}} by condition "where"
	func GetMulti{{.StructName}}(db *gorm.DB, where map[string]interface{}, selectField []string) ([]*{{.StructName}}, error) {
		cond,vals,err := builder.BuildSelect("{{.TableName}}", where, selectField)
		if nil != err {
			return nil, err
		}
		rows, err := db.Raw(cond, vals...).Rows()
		if nil != err || nil == rows {
			return nil, err
		}
		defer rows.Close()
		var res []*{{.StructName}}
		err = scanner.Scan(rows, &res)
		return res,err
	}

	//Create{{.StructName}} inserts an array of data into table {{.TableName}}
	func Create{{.StructName}}(db *gorm.DB, data []map[string]interface{}) (int64, error) {
		cond, vals, err := builder.BuildInsert("{{.TableName}}", data)
		if nil != err {
			return 0, err
		}
		if db, err := db.DB(); err == nil {
		res, err := db.Exec(cond, vals...)
		if nil != err {
			return 0, err
		}
		return res.LastInsertId()
		} else {
			return 0, err
		}
	}

	//Update{{.StructName}} updates the table {{.TableName}}
	func Update{{.StructName}}(db *gorm.DB, where, data map[string]interface{}) (int64, error) {
		cond,vals,err := builder.BuildUpdate("{{.TableName}}", where, data)
		if nil != err {
			return 0, err
		}
		res := db.Exec(cond, vals...)
		if nil != res.Error {
			return 0, err
		}
		return res.RowsAffected, nil
	}

	//Delete{{.StructName}} deletes matched records in {{.TableName}}
	func Delete{{.StructName}}(db *gorm.DB, where map[string]interface{}) (int64, error) {
		cond,vals,err := builder.BuildDelete("{{.TableName}}", where)
		if nil != err {
			return 0, err
		}
		res := db.Exec(cond, vals...)
		if nil != res.Error {
			return 0, err
		}
		return res.RowsAffected, nil
	}
	`
)

type fillData struct {
	StructName string
	TableName  string
}

// GenerateDao generates Dao code
func GenerateDao(tableName, structName string) (io.Reader, error) {
	var buff bytes.Buffer
	err := template.Must(template.New("dao").Parse(daoCode)).Execute(&buff, fillData{
		StructName: structName,
		TableName:  tableName,
	})
	if nil != err {
		return nil, err
	}
	return &buff, nil
}
