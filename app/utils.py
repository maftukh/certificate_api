import re
import uuid
from datetime import datetime

import sqlalchemy as db


NO_EXPIRATION_DATE = datetime(2099, 12, 31)
columns = ['Id', 'name', 'course', 'date', 'expires']


def init_db(name):
    """
    SQLite database initialization
    :param name: name of the table
    :return: engine (to open connections when needed)
             table (instance of the table)
    """
    engine = db.create_engine('sqlite:///certificates.sqlite')
    metadata = db.MetaData()
    table = db.Table(name, metadata,
                     db.Column('Id', db.String, nullable=False, primary_key=True),
                     db.Column('name', db.String, nullable=False),
                     db.Column('course', db.String, nullable=False),
                     db.Column('date', db.DateTime, nullable=False),
                     db.Column('expires', db.DateTime, nullable=False)
                     )
    metadata.create_all(engine)
    return engine, table


class CertificateBase:
    def __init__(self, engine, certificates_table):
        self.engine = engine
        self.certificates_table = certificates_table

    def add(self, data):
        """
        Generate ID and insert data into DB
        :param data: dict with `name`, `course`, `date` and `expires`
        :return: certificate_id
        """
        with self.engine.connect() as connection:
            certificate_id = str(uuid.uuid4())
            query = db.insert(self.certificates_table).values(Id=certificate_id, **data)
            connection.execute(query)
        return certificate_id

    def get(self, certificate_id):
        """
        Get certificate by id from the DB
        :param certificate_id:
        :return: dict with certificate data
                 or None if no data with the given id
        """
        with self.engine.connect() as connection:
            query = db.select([self.certificates_table]).where(self.certificates_table.columns.Id == certificate_id)
            certificate = connection.execute(query).fetchone()
        if certificate:
            return dict(zip(columns, certificate))
        return None

    def delete(self, certificate_id):
        """
        Delete certificate by id if present
        :param certificate_id:
        :return:
        """
        with self.engine.connect() as connection:
            query = db.delete(self.certificates_table).where(self.certificates_table.columns.Id == certificate_id)
            connection.execute(query)

    def get_all(self):
        """
        Get all the certificates from the DB
        :return: list of dicts with certificate data
        """
        with self.engine.connect() as connection:
            query = db.select([self.certificates_table])
            certificates = connection.execute(query).fetchall()
        return [dict(zip(columns, certificate)) for certificate in certificates]


def check_data_validity(name, course, date, expires):
    """
    Check if data is valid

    :param name:
    :param course:
    :param date:
    :param expires:
    :return: True if data is valid (dates are valid, name and course are present)
    """
    if name == '' or name is None:
        return False

    if course == '' or course is None:
        return False

    date_checker = re.compile("^\d{4}-(0?[1-9]|1[0-2])-(0?[1-9]|[12]\d|30|31)$")
    if type(date) is str and not date_checker.match(date):
        return False

    if type(expires) is str and not date_checker.match(expires):
        return False

    return True


def pack_data(args):
    """
    Extract data from the request arguments and pack it into a dict if it's valid
    :param args:
    :return: dict with certificate data,
             or empty dict if data is invalid
    """
    # Get arguments
    name = args.get('name')
    course = args.get('course')
    date = args.get('date', datetime.today().date())
    expires = args.get('expires', NO_EXPIRATION_DATE.date())

    # Check data validity
    if check_data_validity(name, course, date, expires):
        if type(date) is str:
            date = datetime.strptime(date, "%Y-%m-%d").date()
        if type(expires) is str:
            expires = datetime.strptime(expires, "%Y-%m-%d").date()
        data = {'name': name, 'course': course, 'date': date, 'expires': expires}
    else:
        data = {}

    return data
