# Method Usage

This project's goal is to provide insight in the method usage of Python modules based on questions and answers asked on StackOverflow. We believe it is useful for opensource modules to understand how much their methods are being used by the users and which methods are being asked most about on StackOverflow.

## Our approach:
1. Questions and answers are retrieved through the [StackExchange API](https://api.stackexchange.com).
2. This is done daily and the data is stored in a database.
3. Methods of each module are being scraped of their documentation page (example pandas).
4. Based on the methods found in the documentation the body of the questions and answers is being searched to find matches.
5. Output is a dashboard with all kinds of insights about the usage of each module.

## How to run:
1.  Duplicate the `settings_template.yml` file and rename it `settings.yml` which is gitignored. These will be your personal settings
2.  Fill in the settings accordingly:
    1. `schema`: Schema in the database to write to.
    2. `method`: Which method to use when writing new data, `replace` or `append`.
    3. `module`: Which modules to retrieve te data from.
    4. `get_pandas_methods`: Scrape the pandas documentation.
    5. `run_api`: Retrieve new questions and answers from StackExchange.

## Maintainers:

- [Erfan Nariman](https://github.com/erfannariman)
- [Melvin Folkers](https://github.com/melvinfolkers)

## Contributors
### Design:
- Logo designed by [Alejandra Sota](https://www.linkedin.com/in/alejandrasota/).
### Code base: