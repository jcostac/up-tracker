# app.py

from flask import Flask, send_from_directory
from flask_cors import CORS
import mimetypes
import pretty_errors

mimetypes.guess_type("notExists.js")


app = Flask(__name__, static_folder='back/front_dist', static_url_path='/')

#CORS(app, supports_credentials=True, headers='Authorization')


@app.route('/')
def serve_vue_app():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/<path:filename>')
def get_media(filename):    
    return send_from_directory(app.static_folder, filename)


#---------------------------------------------------
# Registrar API REST

from api.api_endpoints import general_bp, programasUP_bp, gananciasUP_bp, programasUOF_bp, gananciasUOF_bp, precios_bp, up_bp, summary_bp
# Register blueprints within the app
app.register_blueprint(general_bp)
app.register_blueprint(programasUP_bp)
app.register_blueprint(gananciasUP_bp)
app.register_blueprint(programasUOF_bp)
app.register_blueprint(gananciasUOF_bp)
app.register_blueprint(precios_bp)
app.register_blueprint(up_bp)
app.register_blueprint(summary_bp)

#----------------------------------------------------

#@app.teardown_appcontext
#def cleanup(resp_or_exc):
#    db.db_session.remove()




if __name__ == "__main__":
    app.run(port=5000, debug=True)