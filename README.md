# iitj-bdm-as6-java-aws-redhat-SQuirrel

[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]

## Assignment Details

The project was assigned from the course **Big Data Management G23AI2028 AS-6**, Assignment No 6.

## Assignment Task

1. Write the method connect() to make a connection to the database. [5]
2. Method close() to close the connection to the database. [5]
3. Method drop() to drop all the tables from the database. Note: The database schema
name will be dev. [5]
4. Method create() to create the database dev and the tables. [5]
5. Write the method insert() to add the standard TPC-H data into the database. The DDL
files are in the ddl folder. Hint: Files are designed so can read entire file as a string and
execute it as one statement. May need to divide up into batches for large files. [10]
6. Write the method query1() that returns the most recent top 10 orders with the total sale
and the date of the order for customers in America. [5]
7. Write the method query2() that returns the customer key and the total price a customer
spent in descending order, for all urgent orders that are not failed for all customers who
are outside Europe and belong to the largest market segment. The largest market
segment is the market segment with the most customers. [10]
8. Write the method query3() that returns a count of all the line items that were ordered
within the six years starting on April 1st, 1997 group by order priority. Make sure to sort
by order priority in ascending order. [10]
9. Try to implement some basic ML techniques for classifying the total price using traditional
techniques such as SVM, Random Forest, Linear Regression using Redshift. This is not
a part of the evaluation but will help you learn the ML features of Redshift.

### Run the code

```
java -cp "bin;lib/*" App
```

### Prerequisites (Minimum)

- AWS RDS DB
- SQL Basic Commands
- VS CODE

### Refereces

- [Oracle VM VirtualBox](https://www.virtualbox.org/)
- [Github](https://github.com)
- [Loom](https://www.loom.com/)

## Authors

👤 **Shubham Raj**

- Github: [@ShubhamRaj](https://github.com/shubham14p3)
- Linkedin: [Shubham14p3](https://www.linkedin.com/in/shubham14p3/)
- Roll No - G23AI2028


## 🤝 Contributing

Contributions, issues and feature requests are welcome!

Feel free to check the [issues page](https://github.com/shubham14p3/vm-g23ai2028-php/issues).

## Show your support

Give a ⭐️ if you like this project!

## Acknowledgments

- Project requested by [IITJ](https://www.iitj.ac.in/).

<!-- MARKDOWN LINKS & IMAGES -->

[contributors-shield]: https://img.shields.io/github/contributors/shubham14p3/members-only.svg?style=flat-square
[contributors-url]: https://github.com/shubham14p3/vm-g23ai2028-php/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/shubham14p3/members-only.svg?style=flat-square
[forks-url]: https://github.com/shubham14p3/vm-g23ai2028-php/network/members
[stars-shield]: https://img.shields.io/github/stars/shubham14p3/members-only.svg?style=flat-square
[stars-url]: https://github.com/shubham14p3/vm-g23ai2028-php/stargazers
[issues-shield]: https://img.shields.io/github/issues/shubham14p3/members-only.svg?style=flat-square
[issues-url]: https://github.com/shubham14p3/vm-g23ai2028-php/issues


