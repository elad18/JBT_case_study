<H1> CWT Test </H1>

<H2> Assumptions </H2> 
Throughout all tasks I have made the assumption that price aggregations do <b>not</b> discount cancellations, as the task sheet did not state otherwise (unless I misread!)


<H2> Environment </H2>
<ul>
<li> WSL2 - Ubuntu 20.04 </li>
<li> Python 3.10.4 </li>
<li> openjdk - 11.0.14.1 </li>
</ul>



<H2> Directory structure </H2>

    tests/
        test_apps.py
        example_dataframes.py # dataframes for testing scenarios
    data/
        results/
            *raw csv files of results*
        bookings.csv
    notebook/
        Q5.ipynb
        Q5.html
    answers/
        answers.jpg
    bookings.py
    main.py
    spark_handler.py


<H2>Executing</H2>
Prior to executing please set up the venv and install the requirements.txt.

    python -m venv .venv
    pip install - r requirements.txt

Executing the below will run the script to print the results of each task into the terminal.

    spark-submit main.py


The entirety of the final task (task number 5) was done in a seperate notebook, for the sake of convenience I've dumped it as a HTML file within the notebooks folder. It was ran within a seperate venv environment just to avoid version conflicts, so to run the notebook please install the q5_requirements.txt.

Also for the sake of convenience, I've dumped my answers as images within the answers folder.

<H2> Testing </H2>

The testing environment is not isolated from the main environment so to execute the tests simply run

    pytest tests/test_app.py

