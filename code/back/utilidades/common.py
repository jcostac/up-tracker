from functools import wraps
#import back.app.models as modelos
from flask import Blueprint, jsonify
from flask import Flask, request
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
from hashlib import sha1
from datetime import datetime, timedelta
#import back.app.config as config


#
# Check password
#----------------------------------------------------------------------
def check_password_hash2(PWDHASH, password):
    print(PWDHASH)
    password_hashed = "*" + sha1(sha1(password.encode('utf-8')).digest()).hexdigest().upper()
    print(password_hashed)
    if PWDHASH == password_hashed:
        return True
    else:
        return False


#
# Response maker
#----------------------------------------------------------------------
def jsend_response_maker(status, data, message):

    response = {}
    response['status'] = status

    if data is not None:
        response['data'] = data

    if message is not None:
        response['message'] = message

    return response


#
# Check token
#----------------------------------------------------------------------
def token_required(f):
    @wraps(f)
    def decorator(*args, **kwargs):

        try:

            token = None

            if 'Authorization' in request.headers:
                token = request.headers['Authorization'].replace("Bearer ","")

            if not token:
                response = jsend_response_maker("fail", None, "A valid token is missing")
                return jsonify(response)

            try:
                data = jwt.decode(token, config.secret_key, algorithms=["HS256"])
            except jwt.ExpiredSignatureError:
                response = jsend_response_maker("fail", None, "Token has expired")
                return jsonify(response)
            except Exception as e:
                response = jsend_response_maker("fail", None, "Error proccesing token")
                return jsonify(response)


            current_user = User.query.filter_by(EMAIL=data['EMAIL']).first()
            print(current_user)

            if not current_user:
                response = jsend_response_maker("fail", None, "Token is invalid")
                return jsonify(response)

        except Exception as e:
            print("--->" + str(e))

        return f(current_user, *args,  **kwargs)
    return decorator




#
# Check user_with_permission_for_task
#----------------------------------------------------------------------
def user_with_permission_for_task(f):
    @wraps(f)
    def decorator(current_user,*args, **kwargs):

        data_input = request.get_json()

        if int(data_input['task_type_id']) == 1:
            if int(data_input['user_id']) != current_user.idUSER:            
                out = {}
                response = jsend_response_maker('warning', out, 'No puedes ejecutar esta acción sobre una tarea personal de otro usuario')
                return jsonify(response)

        if int(data_input['task_type_id']) == 2:
            if not UserTeam.query.filter_by(team_id=int(data_input['team_id'])).filter_by(user_id=int(current_user.idUSER)).first():
                out = {}
                response = jsend_response_maker('warning', out, 'No puedes ejecutar esta acción sobre una tarea de equipo si no perteneces a el')
                return jsonify(response)  

        return f(current_user, *args,  **kwargs)
    return decorator

