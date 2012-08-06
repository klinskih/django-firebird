from django.db.backends import BaseDatabaseIntrospection

class DatabaseIntrospection(BaseDatabaseIntrospection):
    # Maps type codes to Django Field types.
    data_types_reverse = {
        7: 'SmallIntegerField',
        8: 'IntegerField',
        10: 'FloatField',
        12: 'DateField',
        13: 'TimeField',
        14: 'CharField',
        16: 'IntegerField',
        27: 'FloatField',
        35: 'DateTimeField',
        37: 'CharField',
        40: 'TextField',
        261: 'TextField',
        # A NUMERIC/DECIMAL data type is stored as a SMALLINT, INTEGER or BIGINT
        # in Firebird, thus the value of RDB$FIELD_TYPE is reported. So we need
        # two additional helper data types for that to distinguish between real
        # Integer data types and NUMERIC/DECIMAL
        161: 'DecimalField', # NUMERIC => RDB$FIELD_SUB_TYPE = 1
        162: 'DecimalField', # DECIMAL => RDB$FIELD_SUB_TYPE = 2
        # Also, the scale value of a NUMERIC/DECIMAL fields is stored as negative
        # number in the Firebird system	 tables, thus we have to multiply with -1.
        # The SELECT statement in the function get_table_description takes care
        # of all of that.
        170: 'BooleanField',
        171: 'IPField',
        }

    def get_table_list(self, cursor):
        "Returns a list of table names in the current database."
        cursor.execute("""select distinct R.RDB$RELATION_NAME,
                0
from RDB$RELATIONS R
where R.RDB$SYSTEM_FLAG = 0 and R.RDB$VIEW_SOURCE is null and not exists(select 1
                                                                         from RDB$RELATION_CONSTRAINTS RC
                                                                         left join RDB$INDICES I1 on (RC.RDB$INDEX_NAME = I1.RDB$INDEX_NAME)
                                                                         where RC.RDB$RELATION_NAME = R.RDB$RELATION_NAME and RC.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY' and I1.RDB$FOREIGN_KEY not in (select RDB$INDEX_NAME
                                                                                                                                                                                                    from RDB$RELATION_CONSTRAINTS
                                                                                                                                                                                                    where RDB$RELATION_NAME = RC.RDB$RELATION_NAME and RDB$CONSTRAINT_TYPE <> 'FOREIGN KEY' and RDB$INDEX_NAME is not null))
union
select distinct R.RDB$RELATION_NAME,
                1
from RDB$RELATIONS R
where R.RDB$SYSTEM_FLAG = 0 and R.RDB$VIEW_SOURCE is null and exists(select 1
                                                                     from RDB$RELATION_CONSTRAINTS RC
                                                                     left join RDB$INDICES I1 on (RC.RDB$INDEX_NAME = I1.RDB$INDEX_NAME)
                                                                     where RC.RDB$RELATION_NAME = R.RDB$RELATION_NAME and RC.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY' and I1.RDB$FOREIGN_KEY not in (select RDB$INDEX_NAME
                                                                                                                                                                                                from RDB$RELATION_CONSTRAINTS
                                                                                                                                                                                                where RDB$RELATION_NAME = RC.RDB$RELATION_NAME and RDB$CONSTRAINT_TYPE <> 'FOREIGN KEY' and RDB$INDEX_NAME is not null))
order by 2, 1""")
        return [r[0].strip().lower() for r in cursor.fetchall()]

    def table_name_converter(self, name):
        return name.lower()

    def get_table_description(self, cursor, table_name):
        "Returns a description of the table, with the DB-API cursor.description interface."
        tbl_name = "'%s'" % table_name
        sql = """
	    insert into FRAMEWORK_FORM_GROUP_FIELDS (FIELD_NAME, TABLE_NAME, GROUP_ID, FIELD_ORDER, SHOW_ON_FORM, SHOW_ON_FORM_LIST)
	    select
    		case RF.RDB$FIELD_NAME
        	    when 'ID' then 'RID'
        	    else cast(RF.RDB$FIELD_NAME as varchar(1024))
    		end,
    		RF.RDB$RELATION_NAME,
    		1,
    	        RF.RDB$FIELD_POSITION,
    		1,
    		0
		from RDB$RELATION_FIELDS RF
		join RDB$FIELDS F on (RF.RDB$FIELD_SOURCE = F.RDB$FIELD_NAME)
		where RF.RDB$FIELD_NAME not in (select FIELD_NAME
            			                from FRAMEWORK_FORM_GROUP_FIELDS
                            			where TABLE_NAME = RF.RDB$RELATION_NAME)
		order by RF.RDB$RELATION_NAME, RF.RDB$FIELD_POSITION"""
        #cursor.execute(sql)
        cursor.execute("""
            select
		 rf.rdb$field_name
              , case
		    when rf.rdb$field_source = 'FLAGS' then 170
		    when rf.rdb$field_source = 'IP' then 171
		    when (f.rdb$field_type in (7,8,16)) and (f.rdb$field_sub_type > 0) then
                	160 + f.rdb$field_sub_type
                  else
                    f.rdb$field_type end
              , f.rdb$field_length
              , f.rdb$field_precision
              , f.rdb$field_scale * -1
              , rf.rdb$null_flag
		, case 
                    when strpos('//',rf.rdb$description)=0 then rf.rdb$description
                    else left(rf.rdb$description,strpos('//',rf.rdb$description)-2)
                end
              ,case
                    when strpos('//',rf.rdb$description)=0 then null
                    else right(rf.rdb$description,char_length(rf.rdb$description)-strpos('//',rf.rdb$description)-1)
                end
            from
              rdb$relation_fields rf join rdb$fields f on (rf.rdb$field_source = f.rdb$field_name)
            where
              rf.rdb$relation_name = upper(%s)
            order by
              rf.rdb$field_position
            """ % (tbl_name,))
        #from IPython.Shell import IPShellEmbed
        #IPShellEmbed()()
        return [(r[0].strip(), r[1], r[2], r[2] or 0, r[3], r[4], not (r[5] == 1), r[6], r[7]) for r in
                                                                                               cursor.fetchall()]

    def get_relations(self, cursor, table_name):
        """
        Returns a dictionary of {field_index: (field_index_other_table, other_table)}
        representing all relationships to the given table. Indexes are 0-based.
        """
        tbl_name = "'%s'" % table_name
        cursor.execute("""
            select  RF1.RDB$FIELD_POSITION,
                    RF2.RDB$FIELD_POSITION,
                    RF2.RDB$RELATION_NAME
            from RDB$RELATION_CONSTRAINTS RC1
            join RDB$INDICES I1 on (RC1.RDB$INDEX_NAME = I1.RDB$INDEX_NAME)
            join RDB$INDEX_SEGMENTS IS1 on (I1.RDB$INDEX_NAME = IS1.RDB$INDEX_NAME)
            join RDB$RELATION_FIELDS RF1 on (RC1.RDB$RELATION_NAME = RF1.RDB$RELATION_NAME and IS1.RDB$FIELD_NAME = RF1.RDB$FIELD_NAME)
            join RDB$RELATION_CONSTRAINTS RC2 on (RC2.RDB$INDEX_NAME = I1.RDB$FOREIGN_KEY)
            join RDB$INDEX_SEGMENTS IS2 on (RC2.RDB$INDEX_NAME = IS2.RDB$INDEX_NAME)
            join RDB$RELATION_FIELDS RF2 on (RC2.RDB$RELATION_NAME = RF2.RDB$RELATION_NAME and IS2.RDB$FIELD_NAME = RF2.RDB$FIELD_NAME)
            where RF1.RDB$RELATION_NAME = upper(%s) and RC1.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'
            order by RF1.RDB$FIELD_POSITION """ % (tbl_name,))

        relations = {}
        for r in cursor.fetchall():
            relations[r[0]] = (r[1], r[2].strip())
        return relations

    def get_indexes(self, cursor, table_name):
        """
        Returns a dictionary of fieldname -> infodict for the given table,
        where each infodict is in the format:
            {'primary_key': boolean representing whether it's the primary key,
             'unique': boolean representing whether it's a unique index/constraint}
        """

        # This query retrieves each field name and index type on the given table.
        tbl_name = "'%s'" % table_name
        cursor.execute("""
            SELECT
              seg2.rdb$field_name
              , case
                  when exists (
                    select
                      1
                    from
                      rdb$relation_constraints con
                    where
                      con.rdb$constraint_type = 'PRIMARY KEY'
                      and con.rdb$index_name = i.rdb$index_name
                  ) then
                    'PRIMARY KEY'
                  else
                    'UNIQUE'
              end
            FROM
              rdb$indices i
              JOIN rdb$index_segments seg2 on seg2.rdb$index_name = i.rdb$index_name
            WHERE
              i.rdb$relation_name = upper(%s)
              and i.rdb$unique_flag = 1""" % (tbl_name,))
        indexes = {}
        for r in cursor.fetchall():
            indexes[r[0].strip()] = {
                'primary_key': (r[1].strip() == 'PRIMARY KEY'),
                'unique': (r[1].strip() == 'UNIQUE')
            }
        return indexes
