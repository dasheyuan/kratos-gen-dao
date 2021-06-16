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

	func (d *dao) Count{{.StructName}}(ctx context.Context, where map[string]interface{}) (int64, error) {
		cond, vals, err := builder.BuildSelect("{{.TableName}}", where, []string{"COUNT(*)"})
		if nil != err {
			return 0, err
		}
		var total int64
		err = d.db.QueryRow(ctx, cond, vals...).Scan(&total)
		if err != nil {
			return 0, err
		}
		return total,nil
	}

	func (d *dao) GetMulti{{.StructName}}WithPage(ctx context.Context, where map[string]interface{}, selectField []string, page []uint) ([]*{{.StructName}}, int64, error) {
		cond, vals, err := builder.BuildSelect("{{.TableName}}", where, []string{"COUNT(*)"})
		if nil != err {
			return nil, 0, err
		}
		var total int64
		err = d.db.QueryRow(ctx, cond, vals...).Scan(&total)
		if err != nil {
			return nil, 0, err
		}
		if total > 0 {
			where["_limit"] = page
			cond, vals, err := builder.BuildSelect("{{.TableName}}", where, selectField)
			if nil != err {
				return nil, 0, err
			}
			row, err := d.db.Query(ctx, cond, vals...)
			if nil != err || nil == row {
				return nil, 0, err
			}
			defer row.Close()
			var res []*{{.StructName}}
			err = scanner.Scan(row, &res)
			return res, total, err
		}
		return nil, 0, nil
	}

	func Get{{.StructName}}(tx *sql.Tx, where map[string]interface{}, selectField []string) (*{{.StructName}}, error) {
		cond,vals,err := builder.BuildSelect("{{.TableName}}", where, selectField)
		if nil != err {
			return nil, err
		}
		row,err := tx.Query(cond, vals...)
		if nil != err || nil == row {
			return nil, err
		}
		defer row.Close()
		var res *{{.StructName}}
		err = scanner.Scan(row, &res)
		return res,err
	}

	func GetMulti{{.StructName}}(tx *sql.Tx, where map[string]interface{}, selectField []string) ([]*{{.StructName}}, error) {
		cond,vals,err := builder.BuildSelect("{{.TableName}}", where, selectField)
		if nil != err {
			return nil, err
		}
		row,err := tx.Query(cond, vals...)
		if nil != err || nil == row {
			return nil, err
		}
		defer row.Close()
		var res []*{{.StructName}}
		err = scanner.Scan(row, &res)
		return res,err
	}
	
	func Create{{.StructName}}(tx *sql.Tx, data []map[string]interface{}) (int64, error) {
		cond, vals, err := builder.BuildInsert("{{.TableName}}", data)
		if nil != err {
			return 0, err
		}
		result,err := tx.Exec(cond, vals...)
		if nil != err || nil == result {
			return 0, err
		}
		return result.LastInsertId()
	}	

	func Update{{.StructName}}(tx *sql.Tx, where, data map[string]interface{}) (int64, error) {
		cond,vals,err := builder.BuildUpdate("{{.TableName}}", where, data)
		if nil != err {
			return 0, err
		}
		result,err := tx.Exec(cond, vals...)
		if nil != err {
			return 0, err
		}
		return result.RowsAffected()
	}

	func Delete{{.StructName}}(tx *sql.Tx, where map[string]interface{}) (int64, error) {
		cond,vals,err := builder.BuildDelete("{{.TableName}}", where)
		if nil != err {
			return 0, err
		}
		result,err := tx.Exec(cond, vals...)
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
