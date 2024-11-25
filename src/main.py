from quality_evaluation_over_time import QualityEvaluationOT
from punctual_quality_evaluation import PunctualQualityEvaluation
from generate_charts import GenerateCharts
import argparse
from split_lodc_kgs_by_topic import SplitLODCKGsByTopic

TOPICS = ['cross-domain','geography','government','life-sciences','linguistic','media','publications','social-networking','user-generated']

def generate_charts(topic):
    chart_generator_over_time_dimensions = GenerateCharts(f'../data/evaluation_results/{topic}/over_time/by_dimension',f'../charts/{topic}/over_time/by_dimension')
    chart_generator_over_time_dimensions.generate_boxplots_over_time('M')

    chart_generator_over_time_dimensions.swinging_sparql_bubble_chart(f'../data/evaluation_results/{topic}/over_time/by_metric/percentage_of_availability_sparql.csv')

    #Generates a Boxplot for every quality category to see the change in the quality category score over time
    chart_generator_over_time_category = GenerateCharts(f'../data/evaluation_results/{topic}/over_time/by_category',f'../charts/{topic}/over_time/by_category')
    chart_generator_over_time_category.generate_boxplots_over_time('M')

    #Generates a boxplot with category quality score data with measurements over time, at 3-month intervals
    chart_generator_over_time_category.generate_combined_boxplot_over_time('M','Quality by category','category_score_over_time_quarterly')

    #Generates a boxplot with data statistics of all quality dimensions, with point data from the last analysis available
    chart_generator_punctual_dimensions = GenerateCharts(f'../data/evaluation_results/{topic}/punctual',f'../charts/{topic}/punctual')
    chart_generator_punctual_dimensions.generate_boxplots_punctual(f'../data/evaluation_results/{topic}/punctual/dimensions_stats.csv','quality_dimensions')

    #Generates a boxplot with data statistics of all quality categories, with point data from the last analysis available
    chart_generator_punctual_dimensions = GenerateCharts(f'../data/evaluation_results/{topic}/punctual',f'../charts/{topic}/punctual')
    chart_generator_punctual_dimensions.generate_boxplots_punctual(f'../data/evaluation_results/{topic}/punctual/categories_stats.csv','quality_categories','Category')

def filtering():
    #Extract only KGs in the LOD Cloud from the the quality analysis results.
    analysis_over_time = QualityEvaluationOT(f'../data/quality_data/only_from_LODC/all',f'./evaluation_results/all/over_time')
    analysis_over_time.extract_only_lodc('../data/quality_data/all_kgs_analyzed')
    
    # Split the KGs quality data by KGs topic
    by_topic = SplitLODCKGsByTopic()
    #by_topic.recover_lodc_kgs_by_topic()
    by_topic.split_kgs_csv_by_topic('../data/quality_data/all_kgs_analyzed')


def evaluation(topics):
    for topic in topics:
        print(f'Running evaluation for topic: {topic} ...')

        #Load all csv with quality data into pandas df. Results are stored as CSV in the ./evaluation_results/over_time
        analysis_over_time = QualityEvaluationOT(f'../data/quality_data/only_from_LODC/{topic}',f'./evaluation_results/{topic}/over_time')

        #Load csv with the most recent quality analysis avilable. Results are stored as CSV in the ./evaluation_results/punctual
        punctual_analysis = PunctualQualityEvaluation(f'../data/quality_data/only_from_LODC/{topic}/2024-09-29.csv',topic)

        #Evaluate the Availability of the SPARQL endpoint / VoID file / RDF dump
        punctual_analysis.accessibility_stats()

        #Counts the number of KGs that are accessible and have an open license
        punctual_analysis.get_kgs_available_with_license()

        '''
        Due to the best-effort nature of LOD Cloud Quality Analyzer, if the license is not found on LOD Cloud, the DataHub license is entered, 
        so in this case we have considered only the metadata from LOD Cloud, so if used this two functions below directly, the data obtained will be different, since the data used in this case are the raw data calculated by the tool
        in the /evaluation_results/manually_refined_files/ directory, we entered the csv file from which we extracted the data for the paper given as output by the tool after editing.
        '''

        #Calculates the occurrences of the different licenses indicated in the KG metadata.
        punctual_analysis.group_by_value("License machine redeable (metadata)")

        #Compare the license information from the metadata with the license indicated in the KG
        #punctual_analysis.compare_column(['KG id','License machine redeable (metadata)','License machine redeable (query)'],sparql_av=True)


        #Calculates the occurrences of the different serialization formats indicated in the KG metadata
        punctual_analysis.count_elements_by_type('metadata-media-type')

        #Calculates the min, max, mean, q1, q2 for all the quality dimensions monitored.
        punctual_analysis.generate_stats(['Availability score','Licensing score','Interlinking score','Performance score','Accuracy score','Consistency score','Conciseness score',
                        'Verifiability score','Reputation score','Believability score','Volatility score','Completeness score','Amount of data score','Representational-Consistency score','Representational-Conciseness score',
                        'Understandability score','Interpretability score','Versatility score','Security score'],'dimensions_stats',only_sparql_up=True)

        punctual_analysis.generate_stats(['U1-value','CS2-value','IN3-value','RC1-value','RC2-value','N4-value'],'metrics_to_compare_with_luzzu')

        #Extract only the KG with at least SPARQL endpoint, VoID file or RDF dump available and the indication about the license.
        punctual_analysis.get_kgs_available_with_license()

        #Evaluate if there is indication about the KG provenance 
        #(metric used for comparizon with LUZZU, not used in the paper because it was not possible to estimate 
        # the value of this metric from the analyses done by Debattista in 2016 and 2019)
        analysis_over_time.evaluate_provenance_info()

        #Calculates the min, max, mean, q1, q2 for CS1-Entities as member of disjoint class and CS5-Invalid usage of inverse-functional properties, CN2-Extensional conciseness
        #(Used for comparison with LUZZU, along with: U1, CS2, IN3, RC1, RC2, IN4 and CS4 calculated before only on the most recent analysis available)
        analysis_over_time.stats_over_time(['Entities as member of disjoint class','Invalid usage of inverse-functional properties','Deprecated classes/properties used'],'by_metric')
        analysis_over_time.evaluate_conciseness()

        #Analyze the SPARQL endpoint status over time
        #Classify the KG SPARQL endpoint availability over time i.e., whether for a given KG, it was always online, offline, not indicated, or fluctuated in behavior between the 3 states.
        status_df, status_counts, combined_df  = analysis_over_time.classify_sparql_endpoint_availability()

        #For KGs with fluctuating behavior, estimate as a percentage, how many times it was found UP in the reporting period.
        stats, availability_percentage_by_kgid = analysis_over_time.calculate_percentage_of_availability_swinging_sparql(combined_df,status_df)
        analysis_over_time.group_by_availability_percentage(availability_percentage_by_kgid)

        #KGHearBeat only return a quality score for every dimension, this function allows obtaining 
        #the quality score for each of the 6 quality categories defined in literature (the 6 columns will be added to the CSV file).
        analysis_over_time.add_category_score()

        #Evaluate the quality of each category over time, by calculating the q1, min, median, q3, max.
        #(only KGs with the SPARQL endpoint online are considered)
        analysis_over_time.stats_over_time(['Accessibility score','Contextual score','Dataset dynamicity score','Intrinsic score',
                                            'Representational score','Trust score'],'by_category')

        #Evaluate the quality of each category in the punctual analysis, by calculating the q1, min, median, q3, max.
        punctual_analysis = PunctualQualityEvaluation(f'../data/quality_data/only_from_LODC/{topic}/2024-09-29.csv',topic)
        punctual_analysis.generate_stats(['Accessibility score','Contextual score','Dataset dynamicity score','Intrinsic score',
                                            'Representational score','Trust score'],'categories_stats',only_sparql_up=True)

        #Evaluate the quality of each dimension over time, by calculating the q1, min, median, q3, max
        #(only KGs with the SPARQL endpoint online are considered)
        analysis_over_time.stats_over_time([
            'Availability score','Licensing score','Interlinking score','Performance score','Accuracy score','Consistency score','Conciseness score',
            'Verifiability score','Reputation score','Believability score','Currency score','Volatility score','Completeness score','Amount of data score',
            'Representational-Consistency score','Representational-Conciseness score','Understandability score','Interpretability score','Versatility score','Security score'
        ],'by_dimension')

        generate_charts(topic)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Script with parameter -j o --jump_filtering")
    group = parser.add_mutually_exclusive_group()

    group.add_argument("-j", "--jump_filtering", action="store_true", help="If specified, the quality data from the directory ./quality_data will not be filtered by extracting only KGs from LOD CLoud")
    group.add_argument("-c", "--charts_only", action="store_true", help="If specified, the script will only generate charts and skip other processing steps.")
    group.add_argument("-t", "--topics_only", action="store_true", help="If specified, the evaluation will be done by dividing KGs by topic, no overall analysis of the LOD Cloud will be done ")
    group.add_argument("-l", "--all_lodc", action="store_true", help="If specified, the evaluation will be made of the quality of the entire LOD Cloud, without the breakdown of KGs by topic.")
    args = parser.parse_args()

    if(args.jump_filtering == True):
        if(args.topics_only == True):
            evaluation(TOPICS)
        elif(args.all_lodc):
            evaluation('all')
        else:
            TOPICS.append('all')
            evaluation(TOPICS)
    if(args.charts_only == True):
        if(args.topics_only == True):
            generate_charts(TOPICS)
        elif(args.all_lodc):
            generate_charts('all')
        else:
            TOPICS.append('all')
            generate_charts(TOPICS)
    if(args.jump_filtering == False and args.charts_only == False):
        if(args.topics_only == True):
            filtering()
            evaluation(TOPICS)
            generate_charts(TOPICS)
        elif(args.all_lodc):
            filtering()
            evaluation('all')
            generate_charts(TOPICS)
        else:
            filtering()
            TOPICS.append('all')
            evaluation(TOPICS)
