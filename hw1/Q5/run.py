""" run.py - Run the Flask app """
from flaskapp import app

if __name__ == '__main__':
    # Tells Flask to run, accessible from the specified host/port pair. Note
    # that the routes are loaded because of the import above.
    app.run(host='127.0.0.1', port=3001, debug=True)
