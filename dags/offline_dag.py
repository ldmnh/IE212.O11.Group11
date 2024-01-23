# Set environment
import sys, os
sys.path.append(os.path.expanduser('~/code/IE212.O11.Group11'))

# Import libs
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import custom modules
from _constants import *
from _airflow.utils import save_best_model, clear_xcoms
from _spark.preprocess import preprocess_csv
from _spark.train_models import W2V, SVM, RandomForest, LR, GradientBoosted, DecisionTrees
from _spark.predict import calc_predict_acc

default_args = {
	'owner':'Group11',
	'start_date':datetime(2024, 1, 17, 10, 0),
	'retries': 5,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
	dag_id='offline_dag',
	default_args=default_args,
	schedule_interval='@daily'
) as dag:
	# Prepocess train data
	preprocessing_train_set=PythonOperator(
		task_id='train_set_preprocess',
		python_callable=preprocess_csv,
		op_kwargs={
			'data_input_path': TRAIN_SET_PATH,
			'df_fit_path': TRAIN_SET_PATH,
			'data_output_path': TRAIN_PREPROCESSED_PATH,
		}
	)

	# Prepocess test data
	preprocessing_test_set=PythonOperator(
		task_id='test_set_preprocess',
		python_callable=preprocess_csv,
		op_kwargs={
			'data_input_path': TEST_SET_PATH,
			'df_fit_path': TRAIN_SET_PATH,
			'data_output_path': TEST_PREPROCESSED_PATH,
		}
	)

	# Train W2V model
	training_w2v=PythonOperator(
		task_id='train_model_W2V',
		python_callable=W2V,
		op_kwargs={
			'data_input_path': TRAIN_PREPROCESSED_PATH,
			'model_output_path': W2V_MODEL_PATH,
		}
	)

	# Train SVM model
	training_svm=PythonOperator(
		task_id='train_model_SVM',
		python_callable=SVM,
		op_kwargs={
			'data_input_path': TRAIN_PREPROCESSED_PATH,
			'model_w2v_path': W2V_MODEL_PATH,
			'model_output_path': SVM_MODEL_PATH,
		},
		provide_context=True,
	)

	# Predict Acc of SVM model
	predicting_svm=PythonOperator(
		task_id='predict_model_SVM',
		python_callable=calc_predict_acc,
		op_kwargs={
			'data_input_path': TEST_PREPROCESSED_PATH,
		},
		provide_context=True,
	)

	# Train RandomForest model
	training_rf=PythonOperator(
		task_id='train_model_RandomForest',
		python_callable=RandomForest,
		op_kwargs={
			'data_input_path': TRAIN_PREPROCESSED_PATH,
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
			'data_input_path': TEST_PREPROCESSED_PATH,
		},
		provide_context=True,
	)

	# Train LogisticRegression model
	training_lr=PythonOperator(
		task_id='train_model_LogisticRegression',
		python_callable=LR,
		op_kwargs={
			'data_input_path': TRAIN_PREPROCESSED_PATH,
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
			'data_input_path': TEST_PREPROCESSED_PATH,
		},
		provide_context=True,
	)
	
	# Train GradientBoosted model
	training_gb=PythonOperator(
		task_id='train_model_GradientBoosted',
		python_callable=GradientBoosted,
		op_kwargs={
			'data_input_path': TRAIN_PREPROCESSED_PATH,
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
			'data_input_path': TEST_PREPROCESSED_PATH,
		},
		provide_context=True,
	)
	
	# Train DecisionTrees model
	training_dt=PythonOperator(
		task_id='train_model_DecisionTrees',
		python_callable=DecisionTrees,
		op_kwargs={
			'data_input_path': TRAIN_PREPROCESSED_PATH,
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
			'data_input_path': TEST_PREPROCESSED_PATH,
		},
		provide_context=True,
	)

	# Save best Acc model
	saving_model=PythonOperator(
		task_id='save_best_model',
		python_callable=save_best_model,
		provide_context=True,
	)
    
	# Clear Xcoms
	clearing_xcoms=PythonOperator(
		task_id='clear_xcoms',
		python_callable=clear_xcoms,
	)

	[preprocessing_train_set, preprocessing_test_set] \
	>> training_w2v \
	>> training_svm >> predicting_svm \
	>> training_rf >> predicting_rf \
	>> training_lr >> predicting_lr \
	>> training_gb >> predicting_gb \
	>> training_dt >> predicting_dt \
	>> saving_model >> clearing_xcoms