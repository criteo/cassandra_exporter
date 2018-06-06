#!/usr/bin/env python3

import os
import json
from jinja2 import Environment, FileSystemLoader

env = Environment(loader=FileSystemLoader(os.path.dirname(os.path.abspath(__file__))),
                  trim_blocks=True)


def json_validation(template):
    """
    Json validator

    :param template: json template
    :type template: object
    :return: bool
    """

    try:
        json.loads(template)
    except ValueError:
        return False
    return True


def export_file(template, name):
    """
    Export to file generated template

    :param template: json template
    :type template: object
    :param name: file output name
    :type name: str
    :return: bool
    """

    try:
        with open(name, "w") as f:
            f.write(template)
    except ValueError:
        return False
    return True


def valid_and_export(template, dashname):
    """
    Validate and export dashboard from template

    :param template: json template
    :type template: object
    :param dashname: dashboard name
    :type dashname: string
    :return: obj
    """

    if not json_validation(template):
        print('Bad json format for ' + dashname + ' grafana dashboard')
    else:
        if export_file(template, dashname + '.json'):
            print('Successfully generated dashboard: ' + dashname)
        else:
            print('Error during export dashboard: ' + dashname)


def make_default_dashboard():
    template = env.get_template('Cassandra_template.json').render()
    valid_and_export(template, 'cassandra_default')


def make_kubernetes_dashboard():
    template = env.get_template('Cassandra_template.json').render(
        kubernetes=True,
    )
    valid_and_export(template, 'cassandra_kubernetes')


def main():
    make_default_dashboard()
    make_kubernetes_dashboard()


if __name__ == '__main__':
    main()