import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { Movie, MovieCast } from "../shared/types";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  QueryCommand,
  QueryCommandInput,
  GetCommand,
} from "@aws-sdk/lib-dynamodb";
import Ajv from "ajv";
import schema from "../shared/types.schema.json";

type ResponseBody = {
  data: {
    movie: Movie;
    cast?: MovieCast[];
  };
};
// Enable coercion so that the string 'true' is coerced to
// boolean true before validation is performed.
const ajv = new Ajv({ coerceTypes: true });
const isValidQueryParams = ajv.compile(
  schema.definitions["MovieQueryParams"] || {}
);
const ddbDocClient = createDDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
  try {
    // Print Event
    console.log("Event: ", JSON.stringify(event));
    const parameters = event?.pathParameters;
    const movieId = parameters?.movieId
      ? parseInt(parameters.movieId)
      : undefined;

    if (!movieId) {
      return {
        statusCode: 404,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ Message: "Missing movie Id" }),
      };
    }

    const getCommandOutput = await ddbDocClient.send(
      new GetCommand({
        TableName: process.env.MOVIES_TABLE_NAME,
        Key: { movieId: movieId },
      })
    );
    if (!getCommandOutput.Item) {
      return {
        statusCode: 404,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ Message: "Invalid movie Id" }),
      };
    }
    const body: ResponseBody = {
      data: { movie: getCommandOutput.Item as Movie },
    };
    const queryParams = event.queryStringParameters;
    if (isValidQueryParams(queryParams)) {
      let queryCommandInput: QueryCommandInput = {
        TableName: process.env.CAST_TABLE_NAME,
      };
      queryCommandInput = {
        ...queryCommandInput,
        KeyConditionExpression: "movieId = :m",
        ExpressionAttributeValues: {
          ":m": movieId,
        },
      };
      const queryCommandOutput = await ddbDocClient.send(
        new QueryCommand(queryCommandInput)
      );
      body.data.cast = queryCommandOutput.Items as MovieCast[];
    }

    // Return Response
    return {
      statusCode: 200,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(body),
    };
  } catch (error: any) {
    console.log(JSON.stringify(error));
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ error }),
    };
  }
};

function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
