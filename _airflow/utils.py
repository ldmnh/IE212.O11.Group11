# Import libs
import os, shutil
from airflow import settings
from airflow.models import XCom
from sqlalchemy import create_engine

def save_best_model(**kwargs):
    # Pull xcoms
    ti = kwargs['ti']
    xcom_keys = ti.xcom_pull(
        key='predict_acc_result',
        task_ids=[
            'predict_model_SVM',
            'predict_model_RandomForest',
            'predict_model_LogisticRegression',
            'predict_model_GradientBoosted',
            'predict_model_DecisionTrees',
            'predict_model_SVM'
        ], include_prior_dates=True)

    try:
        max_acc = 0
        max_acc_model_path = None

        # Find best model based on accuracy
        for xcom_key in xcom_keys:
            percentage_matching = xcom_key['percentage_matching']
            model_path = xcom_key['model_path']

            if percentage_matching > max_acc:
                max_acc = percentage_matching
                max_acc_model_path = model_path

        # Delete exist best model
        model_folder_path = os.path.expanduser(f'~/code/IE212.O11.Group11/models')
        for folder in os.listdir(os.path.dirname(model_folder_path)):
            if folder.startswith('Best_model_'):
                shutil.rmtree(os.path.join(os.path.dirname(model_folder_path), folder))

        # Create best model folder
        best_model_name = max_acc_model_path.split('/')[-1]
        best_model_path = os.path.expanduser(f'~/code/IE212.O11.Group11/models/Best_model_{best_model_name}_{max_acc}')
        shutil.copytree(max_acc_model_path, best_model_path)
    except Exception as e:
        print('Failed to save best model: %s', e)
        raise

def clear_xcoms():
    try:
        engine = create_engine(settings.SQL_ALCHEMY_CONN)
        with engine.connect() as connection:
            connection.execute(
                XCom.__table__.delete().where(
                    XCom.key.in_(['predict_acc_result', 'model_output_path'])
                )
            )
        print('XCOMs with keys \'predict_acc_result\' and \'model_output_path\' have been deleted successfully.')
    except Exception as e:
        print('Failed to delete XCOMs: %s', e)
        raise