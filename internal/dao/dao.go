package dao

import (
	"bytes"
	"io"
	"text/template"
)

const (
	daoCode = `

	//Get{{.StructName}}(ctx context.Context, where map[string]interface{}, selectField []string) (*{{.StructName}}, error)
	//GetMulti{{.StructName}}(ctx context.Context, where map[string]interface{}, selectField []string) ([]*{{.StructName}}, error)
	//Create{{.StructName}}(ctx context.Context, data []map[string]interface{}) (int64, error)
	//Update{{.StructName}}(ctx context.Context, where, data map[string]interface{}) (int64, error)
	//Delete{{.StructName}}(ctx context.Context, where map[string]interface{}) (int64, error)

	//GetOne gets one record from table {{.TableName}} by condition "where"
	func (d *dao) Get{{.StructName}}(ctx context.Context, where map[string]interface{}, selectField []string) (*{{.StructName}}, error) {
		cond,vals,err := builder.BuildSelect("{{.TableName}}", where, selectField)
		if nil != err {
			return nil, err
		}
		row,err := d.db.Query(ctx, cond, vals...)
		if nil != err || nil == row {
			return nil, err
		}
		defer row.Close()
		var res *{{.StructName}}
		err = scanner.Scan(row, &res)
		return res,err
	}

	//GetMulti gets multiple records from table {{.TableName}} by condition "where"
	func (d *dao) GetMulti{{.StructName}}(ctx context.Context, where map[string]interface{}, selectField []string) ([]*{{.StructName}}, error) {
		cond,vals,err := builder.BuildSelect("{{.TableName}}", where, selectField)
		if nil != err {
			return nil, err
		}
		row,err := d.db.Query(ctx, cond, vals...)
		if nil != err || nil == row {
			return nil, err
		}
		defer row.Close()
		var res []*{{.StructName}}
		err = scanner.Scan(row, &res)
		return res,err
	}

	//Insert inserts an array of data into table {{.TableName}}
	func (d *dao) Create{{.StructName}}(ctx context.Context, data []map[string]interface{}) (int64, error) {
		cond, vals, err := builder.BuildInsert("{{.TableName}}", data)
		if nil != err {
			return 0, err
		}
		result,err := d.db.Exec(ctx, cond, vals...)
		if nil != err || nil == result {
			return 0, err
		}
		return result.LastInsertId()
	}

	//Update updates the table {{.TableName}}
	func (d *dao) Update{{.StructName}}(ctx context.Context, where, data map[string]interface{}) (int64, error) {
		cond,vals,err := builder.BuildUpdate("{{.TableName}}", where, data)
		if nil != err {
			return 0, err
		}
		result,err := d.db.Exec(ctx, cond, vals...)
		if nil != err {
			return 0, err
		}
		return result.RowsAffected()
	}

	// Delete deletes matched records in {{.TableName}}
	func (d *dao) Delete{{.StructName}}(ctx context.Context, where map[string]interface{}) (int64, error) {
		cond,vals,err := builder.BuildDelete("{{.TableName}}", where)
		if nil != err {
			return 0, err
		}
		result,err := d.db.Exec(ctx, cond, vals...)
		if nil != err {
			return 0, err
		}
		return result.RowsAffected()
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
