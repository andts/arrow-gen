package com.andts.arrowgen;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ArrowGenerator {
    private static final byte[] BYTES = "hello".getBytes();

    private int rows;
    private int columns;

    private Schema arrowSchema;
    private VectorSchemaRoot arrowVectorSchemaRoot;
    private ArrowFileWriter arrowFileWriter;
    private RootAllocator allocator;

    private WritableByteChannel writableByteChannel;

    public ArrowGenerator(int columns, int rows, WritableByteChannel writableByteChannel) {
        this.rows = rows;
        this.columns = columns;
        this.writableByteChannel = writableByteChannel;
        this.allocator = new RootAllocator(Integer.MAX_VALUE);
    }

    protected void makeArrowSchema(String colName, Types.MinorType type) throws Exception {
        List<Field> fields = new ArrayList<Field>();
        // generate column number of homogeneous columns
        switch (type) {
            case INT:
                for (int i = 0; i < this.columns; i++) {
                    fields.add(new Field(colName + i,
                        FieldType.nullable(new ArrowType.Int(32, true)), null));
                }
                break;
            case BIGINT:
                for (int i = 0; i < this.columns; i++) {
                    fields.add(new Field(colName + i,
                        FieldType.nullable(new ArrowType.Int(64, true)), null));
                }
                break;
            case FLOAT8:
                for (int i = 0; i < this.columns; i++) {
                    fields.add(new Field(colName + i,
                        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
                }
                break;
            case FLOAT4:
                for (int i = 0; i < this.columns; i++) {
                    fields.add(new Field(colName + i,
                        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null));
                }
                break;
            case VARBINARY:
                for (int i = 0; i < this.columns; i++) {
                    fields.add(new Field(colName + i,
                        FieldType.nullable(new ArrowType.Binary()), null));
                }
                break;
            default:
                throw new Exception(" NYI " + type);
        }
        this.arrowSchema = new Schema(fields, null);
        this.arrowVectorSchemaRoot = VectorSchemaRoot.create(this.arrowSchema, this.allocator);
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
        this.arrowFileWriter = new ArrowFileWriter(this.arrowVectorSchemaRoot, provider, this.writableByteChannel);
    }

    private int fillBatch(int rowCount, FieldVector vector) {
        //TODO generate random values?
        VarBinaryVector binVector = (VarBinaryVector) vector;
        for (int i = 0; i < rowCount; i++) {
            binVector.setSafe(i, BYTES);
        }
        return rowCount;
    }

    public void fillVector() {
        List<FieldVector> fieldVectors = this.arrowVectorSchemaRoot.getFieldVectors();
        try {
            this.arrowFileWriter.start();
            this.arrowVectorSchemaRoot.setRowCount(rows);
            for (int colIdx = 0; colIdx < columns; colIdx++) {
                FieldVector fv = fieldVectors.get(colIdx);
                fv.setInitialCapacity(rows);
                int nonNullrows = fillBatch(rows, fv);
                fv.setValueCount(nonNullrows);
            }
            // once all columns have been generated, write the batch out
            this.arrowFileWriter.writeBatch();
            // close the writer
            this.arrowFileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        Set<StandardOpenOption> options = new HashSet<>();
        options.add(StandardOpenOption.CREATE);
        options.add(StandardOpenOption.APPEND);

        Path file = Paths.get("arrow_bytes.txt");

        SeekableByteChannel byteChannel = Files.newByteChannel(file, options);
        ArrowGenerator arrowGenerator = new ArrowGenerator(5, 100, byteChannel);
        arrowGenerator.makeArrowSchema("col", Types.MinorType.VARBINARY);
        arrowGenerator.fillVector();
    }
}
