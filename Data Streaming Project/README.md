<i>In order to use the repository just:

 $ docker-compose -up -d --build

To stop it:
 
 $ docker-compose down
 </i>
 
<strong>Goal</strong>: Design a single point of information system where a user can get information like cheapest petrol station, latest news, weather, currency exchange rate and trending movies etc.

<strong>Architecture Diagram</strong>

<img src="https://i.imgur.com/uIVCHe7.png"
     alt= "arch diagram"
     style="float:left;margin-right:10px;"/>


<p><strong>Highlights of the Approach:</strong></p>
<ul>
<li>The real time data is collected from five different data sources using python(pykafka).</li>
<li>Data is then stored in a No-SQL database.</li>
<li>For the frontend, Voila and jupyter notebook is used to get a feeling of a standalone web dashboard without using any complex coding or expensive tool.</li>
</ul>
