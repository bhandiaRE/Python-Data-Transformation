{
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "view-in-github",
        "colab_type": "text"
      },
      "source": [
        "<a href=\"https://colab.research.google.com/github/bhandiaRE/Python-Data-Transformation/blob/main/PySpark/transform.py\" target=\"_parent\"><img src=\"https://colab.research.google.com/assets/colab-badge.svg\" alt=\"Open In Colab\"/></a>"
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "from google.colab import drive\n",
        "drive.mount('/content/drive')\n"
      ],
      "metadata": {
        "id": "DUzKFo_2UqhO",
        "outputId": "2e3cac47-74bf-4dbd-cd71-b3a483e694d4",
        "colab": {
          "base_uri": "https://localhost:8080/"
        }
      },
      "execution_count": 1,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Mounted at /content/drive\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "!pip3 install pyspark"
      ],
      "metadata": {
        "id": "6svpgOjNU37a",
        "outputId": "db559fbe-9487-4853-f421-8d3e2330f725",
        "colab": {
          "base_uri": "https://localhost:8080/"
        }
      },
      "execution_count": 2,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Looking in indexes: https://pypi.org/simple, https://us-python.pkg.dev/colab-wheels/public/simple/\n",
            "Collecting pyspark\n",
            "  Downloading pyspark-3.3.1.tar.gz (281.4 MB)\n",
            "\u001b[2K     \u001b[90m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\u001b[0m \u001b[32m281.4/281.4 MB\u001b[0m \u001b[31m4.5 MB/s\u001b[0m eta \u001b[36m0:00:00\u001b[0m\n",
            "\u001b[?25h  Preparing metadata (setup.py) ... \u001b[?25l\u001b[?25hdone\n",
            "Collecting py4j==0.10.9.5\n",
            "  Downloading py4j-0.10.9.5-py2.py3-none-any.whl (199 kB)\n",
            "\u001b[2K     \u001b[90m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\u001b[0m \u001b[32m199.7/199.7 KB\u001b[0m \u001b[31m16.6 MB/s\u001b[0m eta \u001b[36m0:00:00\u001b[0m\n",
            "\u001b[?25hBuilding wheels for collected packages: pyspark\n",
            "  Building wheel for pyspark (setup.py) ... \u001b[?25l\u001b[?25hdone\n",
            "  Created wheel for pyspark: filename=pyspark-3.3.1-py2.py3-none-any.whl size=281845512 sha256=95b35cfe8b063f33d81d8d3f818adc3719a5f4d9e7cb8baa4cd1d1ded407035f\n",
            "  Stored in directory: /root/.cache/pip/wheels/43/dc/11/ec201cd671da62fa9c5cc77078235e40722170ceba231d7598\n",
            "Successfully built pyspark\n",
            "Installing collected packages: py4j, pyspark\n",
            "Successfully installed py4j-0.10.9.5 pyspark-3.3.1\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "from pyspark.sql import SparkSession\n",
        "spark = SparkSession.builder.master('local[*]').getOrCreate()\n",
        "spark"
      ],
      "metadata": {
        "id": "hUluahnKU-xt",
        "outputId": "450eee82-8a2d-4f22-9d6f-de704da2d4be",
        "colab": {
          "base_uri": "https://localhost:8080/",
          "height": 219
        }
      },
      "execution_count": 3,
      "outputs": [
        {
          "output_type": "execute_result",
          "data": {
            "text/plain": [
              "<pyspark.sql.session.SparkSession at 0x7f5373c37be0>"
            ],
            "text/html": [
              "\n",
              "            <div>\n",
              "                <p><b>SparkSession - in-memory</b></p>\n",
              "                \n",
              "        <div>\n",
              "            <p><b>SparkContext</b></p>\n",
              "\n",
              "            <p><a href=\"http://1bbcb5c8fd6c:4040\">Spark UI</a></p>\n",
              "\n",
              "            <dl>\n",
              "              <dt>Version</dt>\n",
              "                <dd><code>v3.3.1</code></dd>\n",
              "              <dt>Master</dt>\n",
              "                <dd><code>local[*]</code></dd>\n",
              "              <dt>AppName</dt>\n",
              "                <dd><code>pyspark-shell</code></dd>\n",
              "            </dl>\n",
              "        </div>\n",
              "        \n",
              "            </div>\n",
              "        "
            ]
          },
          "metadata": {},
          "execution_count": 3
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "df = spark.read.text(\"/content/drive/MyDrive/questionnaire_schema_ru.json\")\n",
        "df.show()"
      ],
      "metadata": {
        "id": "eG9Jw47tVKBP",
        "outputId": "332d5259-8114-4f07-9e19-12c8a7069d30",
        "colab": {
          "base_uri": "https://localhost:8080/"
        }
      },
      "execution_count": 71,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+--------------------+\n",
            "|               value|\n",
            "+--------------------+\n",
            "|                   [|\n",
            "|                   {|\n",
            "|      \"position\": 1,|\n",
            "|    \"text\": \"Usua...|\n",
            "|        \"answers\": [|\n",
            "|                   {|\n",
            "|        \"position...|\n",
            "|        \"text\": \"...|\n",
            "|                  },|\n",
            "|                   {|\n",
            "|        \"position...|\n",
            "|        \"text\": \"...|\n",
            "|                   }|\n",
            "|                   ]|\n",
            "|                  },|\n",
            "|                   {|\n",
            "|      \"position\": 2,|\n",
            "|    \"text\": \"You ...|\n",
            "|        \"answers\": [|\n",
            "|                   {|\n",
            "+--------------------+\n",
            "only showing top 20 rows\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "from pyspark.sql.functions import *\n",
        "\n",
        "split_data = split(df['value'], ':')\n",
        "\n",
        "df = df.withColumn('authors', split_data.getItem(0))\\\n",
        "        .withColumn('Ans', split_data.getItem(1))\\\n",
        "        .drop('value')\n",
        "\n",
        "df.show()\n"
      ],
      "metadata": {
        "id": "i2oe61k9VQIP",
        "outputId": "1bef1651-1987-4114-dded-119296485120",
        "colab": {
          "base_uri": "https://localhost:8080/"
        }
      },
      "execution_count": 72,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+------------------+--------------------+\n",
            "|           authors|                 Ans|\n",
            "+------------------+--------------------+\n",
            "|                 [|                null|\n",
            "|                 {|                null|\n",
            "|        \"position\"|                  1,|\n",
            "|            \"text\"|        \"Usually you|\n",
            "|         \"answers\"|                   [|\n",
            "|                 {|                null|\n",
            "|        \"position\"|                  1,|\n",
            "|            \"text\"|     \"communicative\"|\n",
            "|                },|                null|\n",
            "|                 {|                null|\n",
            "|        \"position\"|                  2,|\n",
            "|            \"text\"| \"quite restraine...|\n",
            "|                 }|                null|\n",
            "|                 ]|                null|\n",
            "|                },|                null|\n",
            "|                 {|                null|\n",
            "|        \"position\"|                  2,|\n",
            "|            \"text\"| \"You more often ...|\n",
            "|         \"answers\"|                   [|\n",
            "|                 {|                null|\n",
            "+------------------+--------------------+\n",
            "only showing top 20 rows\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "from pyspark.sql.functions import *\n",
        "df1 = df.dropna(subset = ['Ans'])\\\n",
        ".withColumn('Ans', regexp_replace('Ans', '1,', 'Answer 1'))\\\n",
        ".withColumn('Ans', regexp_replace('Ans', '2,', 'Answer 2',))\n",
        "df1.show()"
      ],
      "metadata": {
        "id": "9WxuTH3LVZEr",
        "outputId": "caf8495f-8e9d-44c5-8345-5aae7fe3f08e",
        "colab": {
          "base_uri": "https://localhost:8080/"
        }
      },
      "execution_count": 74,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+------------------+--------------------+\n",
            "|           authors|                 Ans|\n",
            "+------------------+--------------------+\n",
            "|        \"position\"|            Answer 1|\n",
            "|            \"text\"|        \"Usually you|\n",
            "|         \"answers\"|                   [|\n",
            "|        \"position\"|            Answer 1|\n",
            "|            \"text\"|     \"communicative\"|\n",
            "|        \"position\"|            Answer 2|\n",
            "|            \"text\"| \"quite restraine...|\n",
            "|        \"position\"|            Answer 2|\n",
            "|            \"text\"| \"You more often ...|\n",
            "|         \"answers\"|                   [|\n",
            "|        \"position\"|            Answer 1|\n",
            "|            \"text\"| \"feelings contro...|\n",
            "|        \"position\"|            Answer 2|\n",
            "|            \"text\"| \"the mind to con...|\n",
            "|        \"position\"|                  3,|\n",
            "|            \"text\"| \"When you decide...|\n",
            "|         \"answers\"|                   [|\n",
            "|        \"position\"|            Answer 1|\n",
            "|            \"text\"| \"plan what and w...|\n",
            "|        \"position\"|            Answer 2|\n",
            "+------------------+--------------------+\n",
            "only showing top 20 rows\n",
            "\n"
          ]
        }
      ]
    }
  ],
  "metadata": {
    "colab": {
      "name": "Welcome To Colaboratory",
      "provenance": [],
      "include_colab_link": true
    },
    "kernelspec": {
      "display_name": "Python 3",
      "name": "python3"
    }
  },
  "nbformat": 4,
  "nbformat_minor": 0
}