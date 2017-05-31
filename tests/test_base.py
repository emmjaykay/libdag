import unittest


class BaseTest(unittest.TestCase):
    js_obj = {
      "storage": "flat_file",
      "start_node": ["ObjectA"],
      "vars": [
        {
          "AWS_ACCESS_KEY": "ACCESS_KEY",
          "AWS_SECRET_KEY": "SECRET_KEY"
        }
      ],
      "graph":
      [
        {
          "name": "ObjectB",
          "location": "my_package.my_module.ObjectB",
          "requires": [
            "ObjectA"
          ]
        },
        {
          "name": "ObjectC",
          "location": "my_package.my_module.ObjectC",
          "requires": [
            "ObjectA"
          ]
        },
        {
          "name": "ObjectA",
          "location": "my_package.my_module.ObjectA",
          "requires": [],
          "input": True
        },
        {
          "name": "ObjectD",
          "location": "my_package.my_module.ObjectD",
          "requires": [
            "ObjectC",
            "ObjectB"
          ],
          "output": True
        }
      ]
    }

