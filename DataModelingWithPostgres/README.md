 

# Data Modeling with PostgreSQL

## Summary

<p>Using a relational database can be very beneficial espescially in context of summarizing data, aggregating data and able to store it in normalized schema can enable that.</p>

<p>This project is developing a dimensional data model (star schema) for our Sparkify data to achieve fast querying to obtain summarized, aggregated data.</p>

<p>I am using PostgreSQL to acheive this. </p>

## Database Schema

<p>I am using a dimensional data model which otherwise called star schema to store the data. Dimensional data models are different from the normalized relational data models. Standard relational data models are primarily used for transaction processing whereas dimensional data models serve analytic processing purposes.</p>

<p>We are using star schema that benefits measuring items and sorting, summarizing using various featuers/attribures. The features of the Sparkify or attributes will be stored in dimensional tables whereas the measures will be stored in fact table.</p>

## ETL Pipeline

<p>I have a ETL piepline that processes data, to fill these tables, by extracting necessary information from the source documents, applying various transformations and loading it to the relational tables.</p>

## How to run the scripts

<ol>
    <li>To create python tables please execute the create_tables.py by > python create_tables.py</li>
    <li>To load the tables (by extracting and transforming) please execute etl.py by > python etl.py </li>
</ol>

