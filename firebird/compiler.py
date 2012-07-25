from itertools import izip
from django.db.models.sql import compiler
from django.db.models.fields import AutoField
from django.db.utils import DatabaseError

class SQLCompiler(compiler.SQLCompiler):
    def as_sql(self, with_limits=True, with_col_aliases=False):
        sql, params = super(SQLCompiler, self).as_sql(with_limits=False, with_col_aliases=with_col_aliases)
        
        if with_limits:
            limits = []
            if self.query.high_mark is not None:
                limits.append('FIRST %d' % (self.query.high_mark - self.query.low_mark))
            if self.query.low_mark:
                if self.query.high_mark is None:
                    val = self.connection.ops.no_limit_value()
                    if val:
                        limits.append('FIRST %d' % val)
                limits.append('SKIP %d' % self.query.low_mark)
            sql = 'SELECT %s %s' % (' '.join(limits), sql[6:].strip())
        return sql, params


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    def _get_seq_name(self, db_table):
        return db_table.upper() + '_SEQ'
    
    def _get_seq_next_value(self, db_table):
        seq_name = self._get_seq_name(db_table)
        if self.connection.ops.firebird_version[0] >= 2:
            seq_txt = 'NEXT VALUE FOR %s' % seq_name
        else:
            seq_txt = 'GEN_ID(%s, 1)' % seq_name
        cursor = self.connection.cursor()
        cursor.execute('SELECT %s FROM rdb$database' % seq_txt)
        id = cursor.fetchone()[0]
        return str(id)
        
    def _get_pk_next_value(self, db_table, pk_column):
        try:
            return self._get_seq_next_value(db_table)
        except DatabaseError:
            cursor = self.connection.cursor()
            cursor.execute('SELECT MAX(%s) FROM %s' % (pk_column, db_table))
            id = cursor.fetchone()[0]
            if not id:
                id = 0
            return id + 1

    def _last_insert_id(self, cursor, model):
        seq_name = self._get_seq_name(model._meta.db_table)
        cursor.execute('SELECT GEN_ID(%s, 0) FROM rdb$database' % seq_name)
        return cursor.fetchone()[0]
    
    def _get_sql(self):
        # We don't need quote_name_unless_alias() here, since these are all
        # going to be column names (so we can avoid the extra overhead).
        qn = self.connection.ops.quote_name
        opts = self.query.model._meta
        result = ['INSERT INTO %s' % qn(opts.db_table)]
        
        has_fields = bool(self.query.fields)
        fields = self.query.fields if has_fields else [opts.pk]
        result.append('(%s)' % ', '.join([qn(f.column) for f in fields]))

        if has_fields:
            params = values = [
                [
                    f.get_db_prep_save(getattr(obj, f.attname) if self.query.raw else f.pre_save(obj, True), connection=self.connection)
                    for f in fields
                ]
                for obj in self.query.objs
            ]
        else:
            values = [[self.connection.ops.pk_default_value()] for obj in self.query.objs]
            params = [[]]
            fields = [None]
        can_bulk = (not any(hasattr(field, "get_placeholder") for field in fields) and
            not self.return_id and self.connection.features.has_bulk_insert)

        if can_bulk:
            placeholders = [["%s"] * len(fields)]
        else:
            placeholders = [
                [self.placeholder(field, v) for field, v in izip(fields, val)]
                for val in values
            ]
        #from IPython.Shell import IPShellEmbed
        #IPShellEmbed()()
        
        if self.return_id and self.connection.features.can_return_id_from_insert:
            params = params[0]
            col = "%s.%s" % (qn(opts.db_table), qn(opts.pk.column))
            result.append("VALUES (%s)" % ", ".join(placeholders[0]))
            r_fmt, r_params = self.connection.ops.return_insert_id()
            result.append(r_fmt % col)
            params += r_params
            return [(" ".join(result), tuple(params))]
        if can_bulk:
            result.append(self.connection.ops.bulk_insert_sql(fields, len(values)))
            return [(" ".join(result), tuple([v for val in values for v in val]))]
        else:
            return [
                (" ".join(result + ["VALUES (%s)" % ", ".join(p)]), vals)
                for p, vals in izip(placeholders, params)
            ]

	#pk_auto = opts.pk and isinstance(opts.pk, AutoField)
        #sql = ['INSERT INTO %s' % qn(opts.db_table)]
        # Build columns names
        #cols = []
        #if pk_auto:
        #    cols.append(qn(opts.pk.column))
        #for c in self.query.columns:
        #self.pre_sql_setup()
        
        #for c in self.get_columns():
        #    cols.append(qn(c))
        #sql.append('(%s)' % ', '.join(cols))
        #has_fields = bool(self.query.fields)
        #fields = self.query.fields if has_fields else [opts.pk]
        #params = values = [
        #    [
        #    f.get_db_prep_save(getattr(obj, f.attname) if self.query.raw else f.pre_save(obj, True), connection=self.connection)
        #    for f in fields
        #    ]
        #    for obj in self.query.objs
        #    ]
        #IPShellEmbed()()
        # Build values placeholders
        #vals = []
        #if pk_auto:
        #    self._pk_val = self._get_pk_next_value(opts.db_table, opts.pk.column)
        #    vals.append(str(self._pk_val))
        #for v in self.query.values:
        #params = ()
        #placeholders = [
        #                [self.placeholder(field, v) for field, v in izip(fields, val)]
        #                for val in values
        #            ]
        #for v in values:
        #    vals.append(self.placeholder(*v))
            #params = params+val[1]
        #sql.append('VALUES (%s)' % ', '.join(vals))
        #sql.append('VALUES (%s)' % ', '.join(placeholders[0]))
        #IPShellEmbed()()
        #params = self.query.params
        #if self.return_id and self.connection.features.can_return_id_from_insert:
        #    col = "%s.%s" % (qn(opts.db_table), qn(opts.pk.column))
        #    r_fmt, r_params = self.connection.ops.return_insert_id()
        #    sql.append(r_fmt % col)
        #    params = params + r_params
            
        #return ' '.join(sql), params
    

    def as_sql(self, *args, **kwargs):
        # Fix for Django ticket #14019
        if not hasattr(self, 'return_id'):
            self.return_id = False

        meta = self.query.get_meta()
       
        if meta.has_auto_field:
            # db_column is None if not explicitly specified by model field
            #auto_field_column = meta.auto_field.db_column or meta.auto_field.column
            #sql, params = 
            return self._get_sql()
            from IPython.Shell import IPShellEmbed
    	    IPShellEmbed()()
        
        else:
            #sql, params = 
            return super(SQLInsertCompiler, self).as_sql(*args, **kwargs)

        #return sql, params
    
    def execute_sql(self, return_id=False):
        self._pk_val = None
        #self.return_id = return_id
        #cursor = super(SQLCompiler, self).execute_sql(None)
        #cursor = super(SQLCompiler, self).execute_sql(self)
        #if not (return_id and cursor):
        #    return
        #if self.connection.features.can_return_id_from_insert:
        #    return self.connection.ops.fetch_returned_insert_id(cursor)
        #if not self._pk_val:
        #    self._pk_val = self._last_insert_id(cursor, self.query.model)
        #return self._pk_val
        
        assert not (return_id and len(self.query.objs) != 1)
        self.return_id = return_id
        cursor = self.connection.cursor()
        for sql, params in self.as_sql():
            cursor.execute(sql, params)
        if not (return_id and cursor):
            return
        if self.connection.features.can_return_id_from_insert:
            return self.connection.ops.fetch_returned_insert_id(cursor)
        return self.connection.ops.last_insert_id(cursor,
                self.query.model._meta.db_table, self.query.model._meta.pk.column)


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass

class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass

class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass

class SQLDateCompiler(compiler.SQLDateCompiler, SQLCompiler):
    pass

