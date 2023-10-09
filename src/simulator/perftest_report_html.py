#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import base64
from pathlib import Path

import pandas as pd

from simulator.log import info
from simulator.perftest_report_common import ReportConfig
from simulator.util import simulator_home, read


class HTMLReport:

    def __init__(self, config: ReportConfig):
        self.metrics = []
        self.config = config

    def make(self):
        image_list = self.__import_images()
        report_df = self.__load_report_csv()

        html_template = read(f"{simulator_home}/src/simulator/report.html")
        file_path = os.path.join(self.config.report_dir + "/report.html")
        file_url = "file://" + file_path
        info(f"Generating HTML report : {file_url}")

        images_index = html_template.index("[images]")
        overview_index = html_template.index("[overview]")
        with (open(file_path, 'w') as f):
            f.write(html_template[0:images_index])

            # Write the images
            for (type, path, title) in image_list:
                with open(path, "rb") as image_file:
                    encoded_image = str(base64.b64encode(image_file.read()), encoding='utf-8')

                encoded_image = "data:image/png;base64,%s" % encoded_image

                image_html = f"""
                    <div class="image-container {type}">
                        <img src="{encoded_image}" onclick="toggleZoom(this);" />
                        <p class="image-text">{title}</p>
                    </div>
                """
                f.write(image_html)
            f.write(html_template[images_index + len("[images]"):overview_index])

            # Write the overview.
            overview = ""
            for column in report_df.columns:
                overview += '<tr><td>' + column + '</td>'
                for value in report_df[column]:
                    overview += '<td>' + str(value) + '</td>'
                overview += '</tr>'
            f.write(overview)

            f.write(html_template[overview_index + len("[overview]"):])

    def __load_report_csv(self):
        path = self.config.report_dir + "/report.csv"
        print(f"\tLoading {path}")
        return pd.read_csv(path)

    def __import_images(self):
        result = []
        result += self.__import_images_dstat()
        result += self.__import_images_operations()
        result += self.__import_images_latency()
        return result

    def __import_images_latency(self):
        result = []
        dir = f"{self.config.report_dir}/latency"

        for outer_filename in os.listdir(dir):
            outer_file_path = f"{dir}/{outer_filename}"
            if not outer_file_path.endswith(".png"):
                continue
            title = Path(outer_file_path).stem.replace('_', ' ')
            result.append(("latency",
                           f"{outer_file_path}",
                           f"{title}"))

        for outer_filename in os.listdir(dir):
            outer_file_path = f"{dir}/{outer_filename}"
            if not os.path.isdir(outer_file_path):
                continue
            for image_filename in os.listdir(outer_file_path):
                if not image_filename.endswith(".png"):
                    continue
                title = Path(image_filename).stem.replace('_', ' ')
                result.append(("latency",
                               f"{outer_file_path}/{image_filename}",
                               f"{outer_filename} {title}"))
        return result

    def __import_images_operations(self):
        result = []
        dir = f"{self.config.report_dir}/operations"

        for outer_filename in os.listdir(dir):
            outer_file_path = f"{dir}/{outer_filename}"
            if not outer_file_path.endswith(".png"):
                continue
            base_filename = Path(outer_file_path).stem
            result.append(("operations",
                           f"{outer_file_path}",
                           f"{base_filename}"))

        for outer_filename in os.listdir(dir):
            outer_file_path = f"{dir}/{outer_filename}"
            if not os.path.isdir(outer_file_path):
                continue
            for image_filename in os.listdir(outer_file_path):
                if not image_filename.endswith(".png"):
                    continue
                base_filename = Path(image_filename).stem
                result.append(("operations",
                               f"{outer_file_path}/{image_filename}",
                               f"{outer_filename} {base_filename}"))
        return result

    def __import_images_dstat(self):
        result = []
        # scan for dstat
        dir = f"{self.config.report_dir}/dstat"
        for agent_filename in os.listdir(dir):
            agent_dir = f"{dir}/{agent_filename}"

            if not os.path.isdir(agent_dir):
                continue
            for image_filename in os.listdir(agent_dir):
                if not image_filename.endswith(".png"):
                    continue
                base_filename = Path(image_filename).stem
                result.append(("dstat", f"{agent_dir}/{image_filename}",
                               f"{agent_filename} {base_filename}"))
        return result
