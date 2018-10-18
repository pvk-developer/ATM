import copy
import operator
import os

from atm.config import load_config
from atm.database import Database


def set_up_db():
    sql_config_path = os.path.join('config', 'sql.yaml')
    sql_conf = load_config(sql_path=sql_config_path)[0]

    # YOU NEED TO redo SQL_CONF path, or get database to accept somethign in a
    # higher directory

    sql_conf.database = os.path.join(sql_conf.database)

    db = Database(sql_conf.dialect, sql_conf.database, sql_conf.username,
                  sql_conf.password, sql_conf.host, sql_conf.port,
                  sql_conf.query)

    return db


class Metaparser:
    def __init__(self, target_entity, db, column_args, op_args=[]):
        self.target_entity = target_entity
        self.column_args = column_args
        self.op_args = op_args
        self.db = db
        self.parser = None

    def set_flaskplus_parser(self, api):
        """ returns a flaskplus api parser for use in the API"""
        temp_parser = api.parser()

        all_args = self.column_args + self.op_args

        for arg in all_args:
            temp_parser.add_argument(
                name=arg.name, type=arg.type, help=arg.help)

        self.parser = temp_parser

    def recode_op_args(self, args=None):
        """
        recoder operation string arguments in the parser to be of type
        operation
        """
        if not args:
            args = self.parser.parse_args()

        for op_arg in self.op_args:
            string_op = args[op_arg.name]
            operation = op_arg.convert_to_operation(string_op)
            args[op_arg.name] = operation

        return args


class Arg:
    def __init__(self, target_col, name, input_type=str, required=False,
                 help_str=''):
        self.name = name
        self.type = input_type
        self.target_col = target_col
        self.required = required
        self.help = help_str


class OpArg(Arg):
    def __init__(self, target_col, name, input_type=str, required=False,
                 help_str=''):
        super().__init__(target_col, name, input_type, required, help_str)

        self.help = 'comparison operator. i.e. =, >, >='

    def convert_to_operation(self, string_rep):
        op_map = {
            '=': operator.eq,
            '>': operator.gt,
            'gt': operator.gt,
            '>=': operator.ge,
            '=>': operator.ge,
            'ge': operator.ge,
            '<': operator.lt,
            'lt': operator.lt,
            '<=': operator.le,
            '=<': operator.le,
            'le': operator.le}

        return op_map.get(string_rep, operator.eq)


db = set_up_db()
ds = db.Dataset


def return_dataset_metaparsers():
    dataset_args = [
        Arg(target_col=ds.id, name='entity_id', input_type=int,
            required=False),
        Arg(ds.name, 'name', str, False),
        Arg(ds.description, 'description', str, False),
        Arg(ds.train_path, 'train_path', str, False),
        Arg(ds.test_path, 'test_path', str, False),
        Arg(ds.class_column, 'class_column', str, False),
        Arg(ds.n_examples, 'n_examples', int, False),
        Arg(ds.k_classes, 'k_classes', int, False),
        Arg(ds.d_features, 'd_features', int, False),
        Arg(ds.majority, 'majority', float, False)]

    operation_args = [
        OpArg(ds.n_examples, 'n_examples_op', str, False),
        OpArg(ds.k_classes, 'k_classes_op', str, False),
        OpArg(ds.d_features, 'd_features_op', str, False),
        OpArg(ds.majority, 'majority_op', str, False),
        OpArg(ds.size_kb, 'size_kb_op', str, False)]

    metaparser_for_dataset_get = Metaparser(
        ds, db, dataset_args, operation_args)
    metaparser_for_dataset_post = Metaparser(ds, db, dataset_args[1:], [])

    new_dataset_args = []
    for arg in dataset_args:
        new_arg = copy.copy(arg)
        new_arg.name = 'new_' + arg.name
        new_dataset_args.append(new_arg)
    new_dataset_args += dataset_args

    metaparser_for_dataset_put = Metaparser(
        ds, db, new_dataset_args, operation_args)

    metaparser_for_dataset_delete = Metaparser(
        ds, db, [Arg(ds.id, name='entity_id', input_type=int, required=True)])

    return {
        'get': metaparser_for_dataset_get,
        'post': metaparser_for_dataset_post,
        'put': metaparser_for_dataset_put,
        'delete': metaparser_for_dataset_delete
    }

dataset_metaparsers = return_dataset_metaparsers() # noqa