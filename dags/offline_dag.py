from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys, os
sys.path.append(os.path.expanduser('~/code/ie212.o11.group11'))

from spark.utils import create_spark_session
from spark.preprocess import preprocess
from spark.train_models import W2V, SVM, RandomForest, LR, GradientBoosted, DecisionTrees
from spark.predict import calc_predict_acc

from dags.utils import save_best_model

default_args = {
	'owner':'group11',
	'start_date':datetime(2024, 1, 17, 10, 0),
	'retries': 5,
    'retry_delay': timedelta(minutes=10),
}

TEST_SET_PATH = os.path.expanduser('~/code/ie212.o11.group11/data/dreaddit-test.csv')
TRAIN_SET_PATH = os.path.expanduser('~/code/ie212.o11.group11/data/dreaddit-train.csv')
PREPROCESSED_PATH = os.path.expanduser('~/code/ie212.o11.group11/data/preprocessed')

W2V_MODEL_PATH = os.path.expanduser('~/code/ie212.o11.group11/models/W2V')
SVM_MODEL_PATH = os.path.expanduser('~/code/ie212.o11.group11/models/SVM')
RANDOM_FOREST_MODEL_PATH = os.path.expanduser('~/code/ie212.o11.group11/models/RandomForest')
LOGISTIC_REGRESSION_MODEL_PATH = os.path.expanduser('~/code/ie212.o11.group11/models/LogisticRegression')
GRADIENT_BOOSTED_MODEL_PATH = os.path.expanduser('~/code/ie212.o11.group11/models/GradientBoosted')
DECISION_TREES_MODEL_PATH = os.path.expanduser('~/code/ie212.o11.group11/models/DecisionTrees')

spark_session = create_spark_session('spark://127.0.0.1:1909', 'Real-time Stress Prediction')

with DAG(
	dag_id='offline_dag',
	default_args=default_args,
	schedule_interval='@daily'
) as dag:
	# Prepocess data
	preprocessing=PythonOperator(
		task_id='preprocess',
		python_callable=preprocess,
		op_kwargs={
			'spark_session': spark_session,
			'data_input_path': TEST_SET_PATH,
			'df_fit_path': TRAIN_SET_PATH,
			'data_output_path': PREPROCESSED_PATH,
		}
	)

	# Train W2V model
	training_w2v=PythonOperator(
		task_id='train_model_W2V',
		python_callable=W2V,
		op_kwargs={
			'spark_session': spark_session,
			'data_input_path': PREPROCESSED_PATH,
			'model_output_path': W2V_MODEL_PATH,
		}
	)

	# Train SVM model
	training_svm=PythonOperator(
		task_id='train_model_SVM',
		python_callable=SVM,
		op_kwargs={
			'spark_session': spark_session,
			'data_input_path': PREPROCESSED_PATH,
			'model_w2v_path': W2V_MODEL_PATH,
			'model_output_path': SVM_MODEL_PATH,
		},
		provide_context=True,
	)

	# Predict Acc of SVM model
	predicting_svm=PythonOperator(
		task_id='predict_model_svm',
		python_callable=calc_predict_acc,
		op_kwargs={
			'spark_session': spark_session,
			'data_input_path': PREPROCESSED_PATH,
		},
		provide_context=True,
	)

	# Train RandomForest model
	training_rf=PythonOperator(
		task_id='train_model_RandomForest',
		python_callable=RandomForest,
		op_kwargs={
			'spark_session': spark_session,
			'data_input_path': PREPROCESSED_PATH,
			'model_w2v_path': W2V_MODEL_PATH,
			'model_output_path': RANDOM_FOREST_MODEL_PATH,
		},
		provide_context=True,
	)

	# Predict Acc of RandomForest model
	predicting_rf=PythonOperator(
		task_id='predict_model_RandomForest',
		python_callable=calc_predict_acc,
		op_kwargs={
			'spark_session': spark_session,
			'data_input_path': PREPROCESSED_PATH,
		},
		provide_context=True,
	)

	# Train LogisticRegression model
	training_lr=PythonOperator(
		task_id='train_model_LogisticRegression',
		python_callable=LR,
		op_kwargs={
			'spark_session': spark_session,
			'data_input_path': PREPROCESSED_PATH,
			'model_w2v_path': W2V_MODEL_PATH,
			'model_output_path': LOGISTIC_REGRESSION_MODEL_PATH,
		},
		provide_context=True,
	)

	# Predict Acc of LogisticRegression model
	predicting_lr=PythonOperator(
		task_id='predict_model_LogisticRegression',
		python_callable=calc_predict_acc,
		op_kwargs={
			'spark_session': spark_session,
			'data_input_path': PREPROCESSED_PATH,
		},
		provide_context=True,
	)
	
	# Train GradientBoosted model
	training_gb=PythonOperator(
		task_id='train_model_GradientBoosted',
		python_callable=GradientBoosted,
		op_kwargs={
			'spark_session': spark_session,
			'data_input_path': PREPROCESSED_PATH,
			'model_w2v_path': W2V_MODEL_PATH,
			'model_output_path': GRADIENT_BOOSTED_MODEL_PATH,
		},
		provide_context=True,
	)

	# Predict Acc of GradientBoosted model
	predicting_gb=PythonOperator(
		task_id='predict_model_GradientBoosted',
		python_callable=calc_predict_acc,
		op_kwargs={
			'spark_session': spark_session,
			'data_input_path': PREPROCESSED_PATH,
		},
		provide_context=True,
	)
	
	# Train DecisionTrees model
	training_dt=PythonOperator(
		task_id='train_model_DecisionTrees',
		python_callable=DecisionTrees,
		op_kwargs={
			'spark_session': spark_session,
			'data_input_path': PREPROCESSED_PATH,
			'model_w2v_path': W2V_MODEL_PATH,
			'model_output_path': DECISION_TREES_MODEL_PATH,
		},
		provide_context=True,
	)

	# Predict Acc of DecisionTrees model
	predicting_dt=PythonOperator(
		task_id='predict_model_DecisionTrees',
		python_callable=calc_predict_acc,
		op_kwargs={
			'spark_session': spark_session,
			'data_input_path': PREPROCESSED_PATH,
		},
		provide_context=True,
	)

	# Save best Acc model
	save_model=PythonOperator(
		task_id='save_best_model',
		python_callable=save_best_model,
		provide_context=True,
	)

	# Main flow
	preprocessing >> training_w2v \
	>> training_svm >> predicting_svm  \
	>> training_rf >> predicting_rf \
	>> training_lr >> predicting_lr \
	>> training_gb >> predicting_gb \
	>> training_dt >> predicting_dt \
	>> save_model