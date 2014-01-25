PYTHONPATH := .:$(PYTHONPATH) 

.PHONY : test

test:
	@PYTHONPATH=$(PYTHONPATH) python test

