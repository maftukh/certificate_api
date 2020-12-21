import logging
from datetime import datetime

from flask import request, jsonify

from app import certificate_app
from app.utils import CertificateBase, init_db, pack_data


logging.basicConfig(filename='logs.log', level=logging.INFO)


engine, certificates_table = init_db('certificate')
database = CertificateBase(engine, certificates_table)


@certificate_app.route('/certificates', methods=['PUT'])
def create_certificate():
    """
    Endpoint for adding certificates to the database
    ------
    :argument (of the request):
    name: Name of the recipient (required param)
    course: Title of the course (required param)
    date: Date when the certificate is given (if missing, given "today")
    expires: Date of certificate expiration (if missing, no expiration (namely, expiration on 2099-12-31))
    :return JSON  {id: id, data: data, status_code: 201}
               or {msg: message, status_code: 400}
                + added location of the certificate in the header (where could it be accessed)
    """
    data = pack_data(request.args)
    if data == {}:
        logging.info(f"Invalid arguments {request.args}")
        response = jsonify(msg="Invalid arguments. 'name' and 'course' are required, " +
                               "'date' and 'expires' should be in a valid YYYY-MM-DD format if present")
        response.status_code = 400
        return response

    certificate_id = database.add(data)

    response = jsonify(id=certificate_id, data=data)
    response.status_code = 201
    response.headers['location'] = f'/certificates/{certificate_id}'

    return response


@certificate_app.route('/certificates', methods=['GET'])
def get_all_certificates():
    """
    Endpoint for reading certificates from the database
    :return List of dicts with certificate data
    """
    db = database.get_all()
    response = jsonify(data=db)
    response.status_code = 200
    return response


@certificate_app.route('/certificates/<certificate_id>', methods=['GET'])
def get_certificate(certificate_id):
    """
    Endpoint for getting a certificate by its ID
    Currently certificate is deleted once it is expired - the logic could be questioned based on the needs

    :param certificate_id: id of a certificate
    :return JSON: {id: id, data: data, status_code: 200}
               or {msg: message, status_code: 404}
    """
    data = database.get(certificate_id)

    if data is not None:
        if data['expires'] >= datetime.today():
            response = jsonify(id=certificate_id, data=data)
            response.status_code = 200

        else:
            # Response depends on what needs to be done when certificate is expired
            # Now I'm assuming that we need to delete it.
            # Alternatively, the certificate could be marked with an "invalid" flag and shown as usual

            logging.info(f'Certificate expired for id {certificate_id}')
            database.delete(certificate_id)
            response = jsonify(msg='Certificate expired')
            response.status_code = 404

    else:
        logging.info(f'No data for id {certificate_id}')
        response = jsonify(msg='Invalid id')
        response.status_code = 404

    return response


@certificate_app.route('/certificates/<certificate_id>', methods=['DELETE'])
def delete_certificate(certificate_id):
    """
    Endpoint for deleting certificates by their ID
    :param certificate_id: id of a certificate
    :return: JSON with a status code (204 - no content, 400 - some error occured)
    """
    response = jsonify()
    database.delete(certificate_id)
    response.status_code = 204

    return response
