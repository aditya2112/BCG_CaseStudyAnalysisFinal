class settings:
    """Project Settings"""
    LOGGING_DIR="resources/logs"
    EXCLUDED_STATES = ['NA','Unknown','Other','UN']
    QUESTION_MAPPING = {
        '1': ['Primary_Person_use.csv'],
        '2': ['Units_use.csv'],
        '3': ['Primary_Person_use.csv','Units_use.csv'],
        '4': ['Primary_Person_use.csv'],
        '5': ['Primary_Person_use.csv'],
        '6': ['Units_use.csv'],
        '7': ['Primary_Person_use.csv','Units_use.csv'],
        '8': ['Primary_Person_use.csv'],
        '9': ['Units_use.csv','Damages.csv'],
        '10': [ 'Charges_use.csv', 'Units_use.csv']

    }

