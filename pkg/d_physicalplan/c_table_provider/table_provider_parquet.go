package tableprovider

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/parquet-go/parquet-go"
	"golang.org/x/sync/errgroup"
	"io"
	"os"
	"strings"
	"time"
	execution "tiny_dataframe/pkg/e_exec_runtime"
	containers "tiny_dataframe/pkg/g_containers"
)

type ParquetTableReader struct {
	filePath string
	schema   containers.ISchema
}

func NewParquetTableReader(filePath string, schema containers.ISchema) (TableReader, error) {
	ds := &ParquetTableReader{filePath: filePath}
	if schema == nil {
		var err error
		schema, err = ds.inferSchema()
		if err != nil {
			return nil, err
		}
	}

	ds.schema = schema
	return ds, nil
}

func (ds *ParquetTableReader) Schema() containers.ISchema {
	return ds.schema
}

func (ds *ParquetTableReader) View(ctx *execution.TaskContext, fn func(ctx *execution.TaskContext, snapshotTs uint64) error) error {
	snapshotTs := uint64(time.Now().UnixNano())
	return fn(ctx, snapshotTs)
}

func (ds *ParquetTableReader) Push(tCtx *execution.TaskContext, snapshotTs uint64, callbacks []Callback, options ...Option) (err error) {
	parquetFile, osFile, err := openParquetFile(ds.filePath)
	if err != nil {
		return err
	}
	defer func(osFile *os.File) {
		err = osFile.Close()
	}(osFile)

	iterOpts := &IterOptions{}
	for _, opt := range options {
		opt(iterOpts)
	}

	rowGroups := make(chan parquet.RowGroup, len(callbacks))

	errG, ctx := errgroup.WithContext(tCtx.Ctx)
	for _, callback := range callbacks {
		callback := callback // to create a copy of callback for each iteration
		errG.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case rg, ok := <-rowGroups:
					if !ok {
						return nil
					}
					var vectors []containers.IVector
					parquetSchema := rg.Schema()
					arrowColDefs := make([]arrow.Field, 0)
					for c, colDef := range parquetSchema.Fields() {
						if !parquetColumnIn(colDef, iterOpts.Projection) {
							continue
						}
						vector, err := parquetColumnToArrowVector(colDef, rg.ColumnChunks()[c])
						if err != nil {
							return err
						}
						vectors = append(vectors, vector)
						arrowColDefs = append(arrowColDefs, arrow.Field{Name: colDef.Name(), Type: vector.DataType()})
					}
					arrowSchema := containers.NewSchema(arrowColDefs, nil)
					batch := containers.NewBatch(arrowSchema, vectors)
					err := callback(ctx, batch)
					if err != nil {
						return err
					}
				}
			}
		})
	}

	errG.Go(func() error {
		for _, rg := range parquetFile.RowGroups() {
			if uint64(1) > snapshotTs {
				//TODO: replace this with the file write Ts.
				continue
			}
			rowGroups <- rg
		}
		close(rowGroups)
		return nil
	})

	return errG.Wait()
}

func parquetColumnToArrowVector(parquetColDef parquet.Field, parquetColumnChunk parquet.ColumnChunk) (containers.IVector, error) {
	var colType arrow.DataType
	colData := make([]any, 0)

	pages := parquetColumnChunk.Pages()
	for {
		page, err := pages.ReadPage()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}

		reader := page.Values()
		// create a buffer to read the page data.
		data := make([]parquet.Value, page.NumValues())
		_, err = reader.ReadValues(data)

		switch parquetColDef.Type().Kind() {
		case parquet.Int32:
			colType = arrow.PrimitiveTypes.Int32
			for _, value := range data {
				colData = append(colData, value.Int32())
			}
		case parquet.Int64:
			colType = arrow.PrimitiveTypes.Int64
			for _, value := range data {
				colData = append(colData, value.Int64())
			}
		default:
			return nil, fmt.Errorf("unsupported type %s", parquetColDef.Type().Kind())
		}
	}
	return containers.NewVector(colType, colData), nil
}

func parquetColumnIn(parquetColDef parquet.Field, projections []string) bool {
	if projections == nil {
		return true
	}
	present := false
	for _, col := range projections {
		if strings.EqualFold(col, parquetColDef.Name()) {
			present = true
			break
		}
	}
	return present
}

func (ds *ParquetTableReader) inferSchema() (schema containers.ISchema, err error) {
	parquetFile, osFile, err := openParquetFile(ds.filePath)
	if err != nil {
		return nil, err
	}
	defer func(f *os.File) {
		err = f.Close()
	}(osFile)

	var fields []arrow.Field
	for _, field := range parquetFile.Schema().Fields() {
		switch field.Type().Kind() {
		case parquet.Int32:
			fields = append(fields, arrow.Field{Name: field.Name(), Type: arrow.PrimitiveTypes.Int32})
		case parquet.Int64:
			fields = append(fields, arrow.Field{Name: field.Name(), Type: arrow.PrimitiveTypes.Int64})
		default:
			return nil, fmt.Errorf("unsupported type %s", field.Type().Kind())
		}
	}
	return containers.NewSchema(fields, nil), nil
}

func openParquetFile(file string) (*parquet.File, *os.File, error) {
	osFile, err := os.Open(file)
	if err != nil {
		return nil, nil, err
	}

	stats, err := osFile.Stat()
	if err != nil {
		return nil, nil, err
	}

	parquetFile, err := parquet.OpenFile(osFile, stats.Size())
	if err != nil {
		return nil, nil, err
	}

	return parquetFile, osFile, nil
}
