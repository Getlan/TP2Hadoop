package Ing2017.TpMapReduce;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App 
{
	public static void main( String[] args ) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = null;
		job = Job.getInstance(conf, "App");
		
		//Initialisation
		job.setJarByClass(App.class);
		job.setMapperClass(PivotMapper.class);
		job.setReducerClass(PivotReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		//Input et output
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/*Données d'entrée du map :
		- numéro de la ligne
		- ligne du fichier .csv sous forme de tableau de valeurs
	   ex : 0,[1;4;8]
	*/
	public static class PivotMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//Variables
			long numeroColonne = 0;
			long numeroLigne = key.get();

			//On sépare la ligne en fonction des ";"
			for (String valeur : value.toString().split(";")) {
				context.write(new LongWritable(numeroColonne), new Text(numeroLigne + "  " + valeur));
				numeroColonne++;
			}
		}
	}
	/*Variables retournées à la fin du map : 
		- numéro de la colonne
		- le numéro de la ligne  et la valeur séparés par deux espaces
		ex : [0,{0  3}]
	*/

	/*Données d'entrée du reduce :
		- numéro de la colonne
		- 2 valeurs : le numéro de ligne et la valeur de la case
	   ex : 0,[{0,3}, {1,6}, {2,2}]
	*/
	public static class PivotReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			//Variables
			long numeroLigne;
			String[] colonneValeur;
			Map<Long,String> output = new TreeMap<Long,String>();
			String outputCSV = "";

			//Pour chaque valeur 
			for (Text v : values) {
				
				//On sépare le numéro de la ligne et la valeur
				colonneValeur = v.toString().split("  ");
				
				//Le numéro de la ligne devient le numéro de la colonne
				long numeroColonne = Long.parseLong(colonneValeur[0]);
				
				//On stocke le numéro de la colonne et la valeur dans la Map
				String valeur = colonneValeur[1];
				output.put(numeroColonne,valeur);
			}
			
			//Le numéro de la colonne devient le numéro de la ligne
			numeroLigne= key.get();
			
			int i = 0;
			
			//Mise en forme des données
			for(Map.Entry<Long,String> kv : output.entrySet()) {
				outputCSV += kv.getValue();
				if(i!=output.size()-1) {
					outputCSV += ";";
				}
				i++;
			}
			context.write(new LongWritable(numeroLigne),  new Text(outputCSV));
		}
		/*Variables retournées à la fin du reduce : 
			- le numéro de la ligne
			- la liste des valeurs contenues dans la ligne
			ex : 0  1;2;3;4
			==> cette méthode permet d'obtenir l'inversion des lignes et colonnes dans une matrice
		 */
	}
}