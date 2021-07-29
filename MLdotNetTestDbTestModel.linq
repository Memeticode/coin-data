<Query Kind="Program">
  <NuGetReference>Microsoft.ML</NuGetReference>
  <NuGetReference>System.Data.SqlClient</NuGetReference>
  <Namespace>Microsoft.ML</Namespace>
  <Namespace>Microsoft.ML.Data</Namespace>
  <Namespace>System.Data.SqlClient</Namespace>
  <Namespace>System.Data.Common</Namespace>
</Query>

void Main()
{
	string cnxn = @"SERVER=DESKTOP-BTN3TLD\TESQL;DATABASE=CoinData;Trusted_Connection=yes;";
	string cmnd = @"
		SELECT 
			is_gain_opportunity	as Label		
			, pct_change_est_avg_price			
			, pct_change_est_avg_trade_volume	
			, sma8_pct_diff						
			, sma21_pct_diff					
			, sma55_pct_diff					
			, sma89_pct_diff					
			, sma144_pct_diff					
			, btc_change_est_avg_price			
			, btc_change_est_avg_trade_volume	
			, btc_sma8_pct_diff					
			, btc_sma21_pct_diff				
			, btc_sma55_pct_diff				
			, btc_sma89_pct_diff				
			, btc_sma144_pct_diff				
			, btc_reg3_b						
			, btc_reg3_rsq						
			, btc_reg3_resid					
			, btc_reg8_b						
			, btc_reg8_rsq						
			, btc_reg8_resid					
			, btc_reg21_b						
			, btc_reg21_rsq						
			, btc_reg21_resid		
		FROM dbo.ModelData_1HrInterval_1DayHold_10PctGain2PctLoss 	
		where 
		case when timestamp <> '1602892800' and product_fk = 51 then 0 else 1 end = 1
	";
	
	var loaderColumns = new DatabaseLoader.Column[]
	{
		//new DatabaseLoader.Column() { Name = "is_gain_opportunity", Type = DbType.Boolean },
		new DatabaseLoader.Column() { Name = "Label", Type = DbType.Boolean },
		new DatabaseLoader.Column() { Name = "pct_change_est_avg_price", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "pct_change_est_avg_trade_volume", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "sma8_pct_diff", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "sma21_pct_diff", Type = DbType.Single},
		new DatabaseLoader.Column() { Name = "sma55_pct_diff", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "sma89_pct_diff", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "sma144_pct_diff", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "btc_change_est_avg_price", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "btc_change_est_avg_trade_volume", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "btc_sma8_pct_diff", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "btc_sma21_pct_diff", Type = DbType.Single},
		new DatabaseLoader.Column() { Name = "btc_sma55_pct_diff", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "btc_sma89_pct_diff", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "btc_sma144_pct_diff", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "btc_reg3_b", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "btc_reg3_rsq", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "btc_reg3_resid", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "btc_reg8_b", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "btc_reg8_rsq", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "btc_reg8_resid", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "btc_reg21_b", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "btc_reg21_rsq", Type = DbType.Single },
		new DatabaseLoader.Column() { Name = "btc_reg21_resid", Type = DbType.Single }
	};

	var connection = new SqlConnection(cnxn);
	var factory = DbProviderFactories.GetFactory(connection);


	var context = new Microsoft.ML.MLContext(seed: 0);
	var loader = context.Data.CreateDatabaseLoader(loaderColumns);
	DatabaseSource dbSource = new DatabaseSource(factory, cnxn, cmnd);
	
	var data = loader.Load(dbSource);
	//preview.Dump();

	var features = data.Schema
		.Select(col => col.Name)
		.Where(colName => colName != "Label")
		.ToArray();

	DataOperationsCatalog.TrainTestData dataSplit = context.Data.TrainTestSplit(data, testFraction: 0.2);
	IDataView trainData = dataSplit.TrainSet;
	IDataView testData = dataSplit.TestSet;

	var pipeline = context.Transforms.Concatenate("Features", features)
		.Append(context.BinaryClassification.Trainers.SdcaLogisticRegression());
	//			featureColumnName: "Features",
	//			labelColumnName: "Label"
	//		));
	
	var model = pipeline.Fit(data);
	
	
	// Eval traditional
	var predictions = model.Transform(testData);
	var metrics = context.BinaryClassification.Evaluate(predictions);
	metrics.Dump();

	
	// Eval actual
	
	//var predictionFunc = context.Model.CreatePredictionEngine<CoinData, CoinDataPrediction>(model);
	//var prediction = predictionFunc.Predict(get1ren());
	//prediction.Dump();
}

public static CoinData get1ren()
{
	return new CoinData
	{
		pct_change_est_avg_price = -0.02482243f,
		pct_change_est_avg_trade_volume = 1.621307f,
		sma8_pct_diff = -0.03318505f,	
		sma21_pct_diff = -0.05218618f,	
		sma55_pct_diff = -0.1102247f,	
		sma89_pct_diff = -0.1088588f,	
		sma144_pct_diff = -0.1182836f,	
		btc_change_est_avg_price = -0.002549284f,	
		btc_change_est_avg_trade_volume = 0.2240779f,
		btc_sma8_pct_diff = -0.002239805f,	
		btc_sma21_pct_diff = -0.002122089f,	
		btc_sma55_pct_diff = -0.006731626f,	
		btc_sma89_pct_diff = -0.007800868f,	
		btc_sma144_pct_diff = -0.008367762f,	
		btc_reg3_b = 0f,	
		btc_reg3_rsq = 0f,	
		btc_reg3_resid = 0f,
		btc_reg8_b = 0f,
		btc_reg8_rsq = 0f,
		btc_reg8_resid = 0f,
		btc_reg21_b = 1.625477f,
		btc_reg21_rsq = 0.2010961f,	
		btc_reg21_resid = -0.01417602f,
	};

}

// DATA CLASSES

// Input
public class CoinData
{
	[LoadColumn(0), ColumnName("Label")]
	public Boolean is_gain_opportunity;
	
	[LoadColumn(1)]
	public Single pct_change_est_avg_price;

	[LoadColumn(2)]
	public Single pct_change_est_avg_trade_volume;

	[LoadColumn(3)]
	public Single sma8_pct_diff;

	[LoadColumn(4)]
	public Single sma21_pct_diff;

	[LoadColumn(5)]
	public Single sma55_pct_diff;

	[LoadColumn(6)]
	public Single sma89_pct_diff;

	[LoadColumn(7)]
	public Single sma144_pct_diff;

	[LoadColumn(8)]
	public Single btc_change_est_avg_price;

	[LoadColumn(9)]
	public Single btc_change_est_avg_trade_volume;

	[LoadColumn(10)]
	public Single btc_sma8_pct_diff;

	[LoadColumn(11)]
	public Single btc_sma21_pct_diff;

	[LoadColumn(12)]
	public Single btc_sma55_pct_diff;

	[LoadColumn(13)]
	public Single btc_sma89_pct_diff;

	[LoadColumn(14)]
	public Single btc_sma144_pct_diff;

	[LoadColumn(15)]
	public Single btc_reg3_b;

	[LoadColumn(16)]
	public Single btc_reg3_rsq;

	[LoadColumn(17)]
	public Single btc_reg3_resid;

	[LoadColumn(18)]
	public Single btc_reg8_b;

	[LoadColumn(19)]
	public Single btc_reg8_rsq;

	[LoadColumn(20)]
	public Single btc_reg8_resid;

	[LoadColumn(21)]
	public Single btc_reg21_b;

	[LoadColumn(22)]
	public Single btc_reg21_rsq;

	[LoadColumn(23)]
	public Single btc_reg21_resid;
}

// Output
public class CoinDataPrediction
{
	//public float Score { get; set; }

	[ColumnName("PredictedLabel")]
	public Boolean Prediction { get; set; }

	public float Probability { get; set; }
}
