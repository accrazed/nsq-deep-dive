# To-dos

* Consider how to unit test different parts of the codebase. Hard to test since we're working directly with a nsq cluster so any intuitive testing is more end-to-end than anything else.

* Consider how async publishes could have results read from with a different CLI setup. Currently `publish` is discrete so waiting for the result chan / continuously running the program causes a delta between the function of the CLI and the intent of the command.
