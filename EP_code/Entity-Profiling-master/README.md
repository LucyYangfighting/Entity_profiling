# hsp
每个样本就是其id号
Input(arg):维度dim，样本个数num_elements，数据特征⽤用 numpy的array数组来表示
Output(邻近节点编号):每个点的空间中最近邻的点都是id表示


# PageRank
Calculate the center of the entity by the pagerank algorithm

#java
Experimental framework of Entity profiling

1.java/RDFDatabase
Parsing raw data(eg:.rdf/.nt/.ttl), the experimental data set is drugbank.nt and building five tables：
    (1)mapping table: Map strings and integers
    (2)types_string table: the type of string_id
    (3)triples_all table: Store all triples
    (4)nodes_type table: Store all  nodes and the type of the node
    (5)types_node table: stroe the type of the all entity

2.java/creat_base_table
Build 3 tables: property_triples_table,property_mid_support,relations_triples.
property_triples_table: Statistics Attribute triple:Subject and object must have type
property_mid_support: Summary table of attribute information,Convenient for later inquiry
relations_triples: Statistics relation triple:Subject and object must have type

3.java/make_string_support
statistical  support value of attribute (the type of the attribute is string)

4.java/Numerical_discretization.java
Continuous numerical discretization：
  1.Equal width division
  2.Local density division
statistical  support value of attribute (the type of the attribute is numerical)

5.java/filter_support
Filter attribute values of support(According to your respective needs)

6.java/property_cosine_similarity
Obtain the vector of the entity through the HSP(H,S,P) model,
Counting the cosine similarity of an entity with a property set

7.java/relation_tag
Use the pagerank algorithm to find the center of each entity,
Use the center of the entity to find the first relationship label

8.java/relation_cosine_similarity
Obtain the vector of the entity through the HSP(H,S,P) model,
Counting the cosine similarity of an entity with a relational set

9.java/make_label
make the tag set after filtering and HSP model cosine_similarity

10.java/resort_labels
Label-set reordering,distinctive labels are listed in front,make final tags set

11.java/attach_tags
Label the entity after creating the final tag set

12.java/make_tag_value
Convert abstract tags from the SQLite into database real information


