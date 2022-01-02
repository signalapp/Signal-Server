package org.whispersystems.textsecuregcm.storage;

import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.awscore.client.handler.AwsSyncClientHandler;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ApiName;
import software.amazon.awssdk.core.RequestOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.core.client.handler.ClientExecutionParams;
import software.amazon.awssdk.core.client.handler.SyncClientHandler;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.http.HttpResponseHandler;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.core.util.VersionInfo;
import software.amazon.awssdk.metrics.MetricCollector;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.NoOpMetricCollector;
import software.amazon.awssdk.protocols.core.ExceptionMetadata;
import software.amazon.awssdk.protocols.json.AwsJsonProtocol;
import software.amazon.awssdk.protocols.json.AwsJsonProtocolFactory;
import software.amazon.awssdk.protocols.json.AwsJsonProtocolFactory.Builder;
import software.amazon.awssdk.protocols.json.BaseAwsJsonProtocolFactory;
import software.amazon.awssdk.protocols.json.JsonOperationMetadata;
import software.amazon.awssdk.services.appconfig.AppConfigClient;
import software.amazon.awssdk.services.appconfig.model.AppConfigException;
import software.amazon.awssdk.services.appconfig.model.AppConfigRequest;
import software.amazon.awssdk.services.appconfig.model.BadRequestException;
import software.amazon.awssdk.services.appconfig.model.ConflictException;
import software.amazon.awssdk.services.appconfig.model.CreateApplicationRequest;
import software.amazon.awssdk.services.appconfig.model.CreateApplicationResponse;
import software.amazon.awssdk.services.appconfig.model.CreateConfigurationProfileRequest;
import software.amazon.awssdk.services.appconfig.model.CreateConfigurationProfileResponse;
import software.amazon.awssdk.services.appconfig.model.CreateDeploymentStrategyRequest;
import software.amazon.awssdk.services.appconfig.model.CreateDeploymentStrategyResponse;
import software.amazon.awssdk.services.appconfig.model.CreateEnvironmentRequest;
import software.amazon.awssdk.services.appconfig.model.CreateEnvironmentResponse;
import software.amazon.awssdk.services.appconfig.model.CreateHostedConfigurationVersionRequest;
import software.amazon.awssdk.services.appconfig.model.CreateHostedConfigurationVersionResponse;
import software.amazon.awssdk.services.appconfig.model.DeleteApplicationRequest;
import software.amazon.awssdk.services.appconfig.model.DeleteApplicationResponse;
import software.amazon.awssdk.services.appconfig.model.DeleteConfigurationProfileRequest;
import software.amazon.awssdk.services.appconfig.model.DeleteConfigurationProfileResponse;
import software.amazon.awssdk.services.appconfig.model.DeleteDeploymentStrategyRequest;
import software.amazon.awssdk.services.appconfig.model.DeleteDeploymentStrategyResponse;
import software.amazon.awssdk.services.appconfig.model.DeleteEnvironmentRequest;
import software.amazon.awssdk.services.appconfig.model.DeleteEnvironmentResponse;
import software.amazon.awssdk.services.appconfig.model.DeleteHostedConfigurationVersionRequest;
import software.amazon.awssdk.services.appconfig.model.DeleteHostedConfigurationVersionResponse;
import software.amazon.awssdk.services.appconfig.model.GetApplicationRequest;
import software.amazon.awssdk.services.appconfig.model.GetApplicationResponse;
import software.amazon.awssdk.services.appconfig.model.GetConfigurationProfileRequest;
import software.amazon.awssdk.services.appconfig.model.GetConfigurationProfileResponse;
import software.amazon.awssdk.services.appconfig.model.GetConfigurationRequest;
import software.amazon.awssdk.services.appconfig.model.GetConfigurationResponse;
import software.amazon.awssdk.services.appconfig.model.GetDeploymentRequest;
import software.amazon.awssdk.services.appconfig.model.GetDeploymentResponse;
import software.amazon.awssdk.services.appconfig.model.GetDeploymentStrategyRequest;
import software.amazon.awssdk.services.appconfig.model.GetDeploymentStrategyResponse;
import software.amazon.awssdk.services.appconfig.model.GetEnvironmentRequest;
import software.amazon.awssdk.services.appconfig.model.GetEnvironmentResponse;
import software.amazon.awssdk.services.appconfig.model.GetHostedConfigurationVersionRequest;
import software.amazon.awssdk.services.appconfig.model.GetHostedConfigurationVersionResponse;
import software.amazon.awssdk.services.appconfig.model.InternalServerException;
import software.amazon.awssdk.services.appconfig.model.ListApplicationsRequest;
import software.amazon.awssdk.services.appconfig.model.ListApplicationsResponse;
import software.amazon.awssdk.services.appconfig.model.ListConfigurationProfilesRequest;
import software.amazon.awssdk.services.appconfig.model.ListConfigurationProfilesResponse;
import software.amazon.awssdk.services.appconfig.model.ListDeploymentStrategiesRequest;
import software.amazon.awssdk.services.appconfig.model.ListDeploymentStrategiesResponse;
import software.amazon.awssdk.services.appconfig.model.ListDeploymentsRequest;
import software.amazon.awssdk.services.appconfig.model.ListDeploymentsResponse;
import software.amazon.awssdk.services.appconfig.model.ListEnvironmentsRequest;
import software.amazon.awssdk.services.appconfig.model.ListEnvironmentsResponse;
import software.amazon.awssdk.services.appconfig.model.ListHostedConfigurationVersionsRequest;
import software.amazon.awssdk.services.appconfig.model.ListHostedConfigurationVersionsResponse;
import software.amazon.awssdk.services.appconfig.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.appconfig.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.appconfig.model.PayloadTooLargeException;
import software.amazon.awssdk.services.appconfig.model.ResourceNotFoundException;
import software.amazon.awssdk.services.appconfig.model.ServiceQuotaExceededException;
import software.amazon.awssdk.services.appconfig.model.StartDeploymentRequest;
import software.amazon.awssdk.services.appconfig.model.StartDeploymentResponse;
import software.amazon.awssdk.services.appconfig.model.StopDeploymentRequest;
import software.amazon.awssdk.services.appconfig.model.StopDeploymentResponse;
import software.amazon.awssdk.services.appconfig.model.TagResourceRequest;
import software.amazon.awssdk.services.appconfig.model.TagResourceResponse;
import software.amazon.awssdk.services.appconfig.model.UntagResourceRequest;
import software.amazon.awssdk.services.appconfig.model.UntagResourceResponse;
import software.amazon.awssdk.services.appconfig.model.UpdateApplicationRequest;
import software.amazon.awssdk.services.appconfig.model.UpdateApplicationResponse;
import software.amazon.awssdk.services.appconfig.model.UpdateConfigurationProfileRequest;
import software.amazon.awssdk.services.appconfig.model.UpdateConfigurationProfileResponse;
import software.amazon.awssdk.services.appconfig.model.UpdateDeploymentStrategyRequest;
import software.amazon.awssdk.services.appconfig.model.UpdateDeploymentStrategyResponse;
import software.amazon.awssdk.services.appconfig.model.UpdateEnvironmentRequest;
import software.amazon.awssdk.services.appconfig.model.UpdateEnvironmentResponse;
import software.amazon.awssdk.services.appconfig.model.ValidateConfigurationRequest;
import software.amazon.awssdk.services.appconfig.model.ValidateConfigurationResponse;
import software.amazon.awssdk.services.appconfig.paginators.ListApplicationsIterable;
import software.amazon.awssdk.services.appconfig.paginators.ListConfigurationProfilesIterable;
import software.amazon.awssdk.services.appconfig.paginators.ListDeploymentStrategiesIterable;
import software.amazon.awssdk.services.appconfig.paginators.ListDeploymentsIterable;
import software.amazon.awssdk.services.appconfig.paginators.ListEnvironmentsIterable;
import software.amazon.awssdk.services.appconfig.paginators.ListHostedConfigurationVersionsIterable;
import software.amazon.awssdk.services.appconfig.transform.CreateApplicationRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.CreateConfigurationProfileRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.CreateDeploymentStrategyRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.CreateEnvironmentRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.CreateHostedConfigurationVersionRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.DeleteApplicationRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.DeleteConfigurationProfileRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.DeleteDeploymentStrategyRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.DeleteEnvironmentRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.DeleteHostedConfigurationVersionRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.GetApplicationRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.GetConfigurationProfileRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.GetConfigurationRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.GetDeploymentRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.GetDeploymentStrategyRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.GetEnvironmentRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.GetHostedConfigurationVersionRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.ListApplicationsRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.ListConfigurationProfilesRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.ListDeploymentStrategiesRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.ListDeploymentsRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.ListEnvironmentsRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.ListHostedConfigurationVersionsRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.ListTagsForResourceRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.StartDeploymentRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.StopDeploymentRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.TagResourceRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.UntagResourceRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.UpdateApplicationRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.UpdateConfigurationProfileRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.UpdateDeploymentStrategyRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.UpdateEnvironmentRequestMarshaller;
import software.amazon.awssdk.services.appconfig.transform.ValidateConfigurationRequestMarshaller;
import software.amazon.awssdk.utils.Logger;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class NewAppConfigClient implements AppConfigClient {

  private static final Logger log = Logger.loggerFor(NewAppConfigClient.class);
  private final SyncClientHandler clientHandler;
  private final AwsJsonProtocolFactory protocolFactory;
  private final SdkClientConfiguration clientConfiguration;

  public NewAppConfigClient(SdkClientConfiguration clientConfiguration) {
    this.clientHandler = new AwsSyncClientHandler(clientConfiguration);
    this.clientConfiguration = clientConfiguration;
    this.protocolFactory = ((Builder) this.init(AwsJsonProtocolFactory.builder())).build();
  }

  @Override
  public final String serviceName() {
    return SERVICE_NAME;
  }

  @Override
  public CreateApplicationResponse createApplication(CreateApplicationRequest createApplicationRequest)
      throws BadRequestException, InternalServerException, AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<CreateApplicationResponse> responseHandler = protocolFactory.createResponseHandler(operationMetadata,
        CreateApplicationResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, createApplicationRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "CreateApplication");

      return clientHandler.execute(new ClientExecutionParams<CreateApplicationRequest, CreateApplicationResponse>()
          .withOperationName("CreateApplication").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(createApplicationRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new CreateApplicationRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public CreateConfigurationProfileResponse createConfigurationProfile(
      CreateConfigurationProfileRequest createConfigurationProfileRequest) throws BadRequestException,
      ResourceNotFoundException, InternalServerException, AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<CreateConfigurationProfileResponse> responseHandler = protocolFactory.createResponseHandler(
        operationMetadata, CreateConfigurationProfileResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, createConfigurationProfileRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "CreateConfigurationProfile");

      return clientHandler
          .execute(new ClientExecutionParams<CreateConfigurationProfileRequest, CreateConfigurationProfileResponse>()
              .withOperationName("CreateConfigurationProfile").withResponseHandler(responseHandler)
              .withErrorResponseHandler(errorResponseHandler).withInput(createConfigurationProfileRequest)
              .withMetricCollector(apiCallMetricCollector)
              .withMarshaller(new CreateConfigurationProfileRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }

  @Override
  public CreateDeploymentStrategyResponse createDeploymentStrategy(
      CreateDeploymentStrategyRequest createDeploymentStrategyRequest) throws InternalServerException, BadRequestException,
      AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<CreateDeploymentStrategyResponse> responseHandler = protocolFactory.createResponseHandler(
        operationMetadata, CreateDeploymentStrategyResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, createDeploymentStrategyRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "CreateDeploymentStrategy");

      return clientHandler
          .execute(new ClientExecutionParams<CreateDeploymentStrategyRequest, CreateDeploymentStrategyResponse>()
              .withOperationName("CreateDeploymentStrategy").withResponseHandler(responseHandler)
              .withErrorResponseHandler(errorResponseHandler).withInput(createDeploymentStrategyRequest)
              .withMetricCollector(apiCallMetricCollector)
              .withMarshaller(new CreateDeploymentStrategyRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }

  @Override
  public CreateEnvironmentResponse createEnvironment(CreateEnvironmentRequest createEnvironmentRequest)
      throws InternalServerException, ResourceNotFoundException, BadRequestException, AwsServiceException,
      SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<CreateEnvironmentResponse> responseHandler = protocolFactory.createResponseHandler(operationMetadata,
        CreateEnvironmentResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, createEnvironmentRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "CreateEnvironment");

      return clientHandler.execute(new ClientExecutionParams<CreateEnvironmentRequest, CreateEnvironmentResponse>()
          .withOperationName("CreateEnvironment").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(createEnvironmentRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new CreateEnvironmentRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }

  @Override
  public CreateHostedConfigurationVersionResponse createHostedConfigurationVersion(
      CreateHostedConfigurationVersionRequest createHostedConfigurationVersionRequest) throws BadRequestException,
      ServiceQuotaExceededException, ResourceNotFoundException, ConflictException, PayloadTooLargeException,
      InternalServerException, AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(false).build();

    HttpResponseHandler<CreateHostedConfigurationVersionResponse> responseHandler = protocolFactory.createResponseHandler(
        operationMetadata, CreateHostedConfigurationVersionResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration,
        createHostedConfigurationVersionRequest.overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "CreateHostedConfigurationVersion");

      return clientHandler
          .execute(new ClientExecutionParams<CreateHostedConfigurationVersionRequest, CreateHostedConfigurationVersionResponse>()
              .withOperationName("CreateHostedConfigurationVersion").withResponseHandler(responseHandler)
              .withErrorResponseHandler(errorResponseHandler).withInput(createHostedConfigurationVersionRequest)
              .withMetricCollector(apiCallMetricCollector)
              .withMarshaller(new CreateHostedConfigurationVersionRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }

  @Override
  public DeleteApplicationResponse deleteApplication(DeleteApplicationRequest deleteApplicationRequest)
      throws ResourceNotFoundException, InternalServerException, BadRequestException, AwsServiceException,
      SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<DeleteApplicationResponse> responseHandler = protocolFactory.createResponseHandler(operationMetadata,
        DeleteApplicationResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, deleteApplicationRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "DeleteApplication");

      return clientHandler.execute(new ClientExecutionParams<DeleteApplicationRequest, DeleteApplicationResponse>()
          .withOperationName("DeleteApplication").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(deleteApplicationRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new DeleteApplicationRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }

  @Override
  public DeleteConfigurationProfileResponse deleteConfigurationProfile(
      DeleteConfigurationProfileRequest deleteConfigurationProfileRequest) throws ResourceNotFoundException,
      ConflictException, InternalServerException, BadRequestException, AwsServiceException, SdkClientException,
      AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<DeleteConfigurationProfileResponse> responseHandler = protocolFactory.createResponseHandler(
        operationMetadata, DeleteConfigurationProfileResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, deleteConfigurationProfileRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "DeleteConfigurationProfile");

      return clientHandler
          .execute(new ClientExecutionParams<DeleteConfigurationProfileRequest, DeleteConfigurationProfileResponse>()
              .withOperationName("DeleteConfigurationProfile").withResponseHandler(responseHandler)
              .withErrorResponseHandler(errorResponseHandler).withInput(deleteConfigurationProfileRequest)
              .withMetricCollector(apiCallMetricCollector)
              .withMarshaller(new DeleteConfigurationProfileRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }

  @Override
  public DeleteDeploymentStrategyResponse deleteDeploymentStrategy(
      DeleteDeploymentStrategyRequest deleteDeploymentStrategyRequest) throws ResourceNotFoundException,
      InternalServerException, BadRequestException, AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<DeleteDeploymentStrategyResponse> responseHandler = protocolFactory.createResponseHandler(
        operationMetadata, DeleteDeploymentStrategyResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, deleteDeploymentStrategyRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "DeleteDeploymentStrategy");

      return clientHandler
          .execute(new ClientExecutionParams<DeleteDeploymentStrategyRequest, DeleteDeploymentStrategyResponse>()
              .withOperationName("DeleteDeploymentStrategy").withResponseHandler(responseHandler)
              .withErrorResponseHandler(errorResponseHandler).withInput(deleteDeploymentStrategyRequest)
              .withMetricCollector(apiCallMetricCollector)
              .withMarshaller(new DeleteDeploymentStrategyRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public DeleteEnvironmentResponse deleteEnvironment(DeleteEnvironmentRequest deleteEnvironmentRequest)
      throws ResourceNotFoundException, ConflictException, InternalServerException, BadRequestException,
      AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<DeleteEnvironmentResponse> responseHandler = protocolFactory.createResponseHandler(operationMetadata,
        DeleteEnvironmentResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, deleteEnvironmentRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "DeleteEnvironment");

      return clientHandler.execute(new ClientExecutionParams<DeleteEnvironmentRequest, DeleteEnvironmentResponse>()
          .withOperationName("DeleteEnvironment").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(deleteEnvironmentRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new DeleteEnvironmentRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public DeleteHostedConfigurationVersionResponse deleteHostedConfigurationVersion(
      DeleteHostedConfigurationVersionRequest deleteHostedConfigurationVersionRequest) throws BadRequestException,
      ResourceNotFoundException, InternalServerException, AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<DeleteHostedConfigurationVersionResponse> responseHandler = protocolFactory.createResponseHandler(
        operationMetadata, DeleteHostedConfigurationVersionResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration,
        deleteHostedConfigurationVersionRequest.overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "DeleteHostedConfigurationVersion");

      return clientHandler
          .execute(new ClientExecutionParams<DeleteHostedConfigurationVersionRequest, DeleteHostedConfigurationVersionResponse>()
              .withOperationName("DeleteHostedConfigurationVersion").withResponseHandler(responseHandler)
              .withErrorResponseHandler(errorResponseHandler).withInput(deleteHostedConfigurationVersionRequest)
              .withMetricCollector(apiCallMetricCollector)
              .withMarshaller(new DeleteHostedConfigurationVersionRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public GetApplicationResponse getApplication(GetApplicationRequest getApplicationRequest) throws ResourceNotFoundException,
      InternalServerException, BadRequestException, AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<GetApplicationResponse> responseHandler = protocolFactory.createResponseHandler(operationMetadata,
        GetApplicationResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, getApplicationRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "GetApplication");

      return clientHandler.execute(new ClientExecutionParams<GetApplicationRequest, GetApplicationResponse>()
          .withOperationName("GetApplication").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(getApplicationRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new GetApplicationRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public GetConfigurationResponse getConfiguration(GetConfigurationRequest getConfigurationRequest)
      throws ResourceNotFoundException, InternalServerException, BadRequestException, AwsServiceException,
      SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(false).build();

    HttpResponseHandler<GetConfigurationResponse> responseHandler = protocolFactory.createResponseHandler(operationMetadata,
        GetConfigurationResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, getConfigurationRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "GetLatestConfiguration");

      return clientHandler.execute(new ClientExecutionParams<GetConfigurationRequest, GetConfigurationResponse>()
          .withOperationName("GetLatestConfiguration").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(getConfigurationRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new GetConfigurationRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public GetConfigurationProfileResponse getConfigurationProfile(
      GetConfigurationProfileRequest getConfigurationProfileRequest)
      throws ResourceNotFoundException, InternalServerException, BadRequestException, AwsServiceException,
      SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<GetConfigurationProfileResponse> responseHandler = protocolFactory.createResponseHandler(
        operationMetadata, GetConfigurationProfileResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, getConfigurationProfileRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "GetConfigurationProfile");

      return clientHandler
          .execute(new ClientExecutionParams<GetConfigurationProfileRequest, GetConfigurationProfileResponse>()
              .withOperationName("GetConfigurationProfile").withResponseHandler(responseHandler)
              .withErrorResponseHandler(errorResponseHandler).withInput(getConfigurationProfileRequest)
              .withMetricCollector(apiCallMetricCollector)
              .withMarshaller(new GetConfigurationProfileRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public GetDeploymentResponse getDeployment(GetDeploymentRequest getDeploymentRequest) throws ResourceNotFoundException,
      InternalServerException, BadRequestException, AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<GetDeploymentResponse> responseHandler = protocolFactory.createResponseHandler(operationMetadata,
        GetDeploymentResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, getDeploymentRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "GetDeployment");

      return clientHandler.execute(new ClientExecutionParams<GetDeploymentRequest, GetDeploymentResponse>()
          .withOperationName("GetDeployment").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(getDeploymentRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new GetDeploymentRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public GetDeploymentStrategyResponse getDeploymentStrategy(GetDeploymentStrategyRequest getDeploymentStrategyRequest)
      throws ResourceNotFoundException, InternalServerException, BadRequestException, AwsServiceException,
      SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<GetDeploymentStrategyResponse> responseHandler = protocolFactory.createResponseHandler(
        operationMetadata, GetDeploymentStrategyResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, getDeploymentStrategyRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "GetDeploymentStrategy");

      return clientHandler.execute(new ClientExecutionParams<GetDeploymentStrategyRequest, GetDeploymentStrategyResponse>()
          .withOperationName("GetDeploymentStrategy").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(getDeploymentStrategyRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new GetDeploymentStrategyRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public GetEnvironmentResponse getEnvironment(GetEnvironmentRequest getEnvironmentRequest) throws ResourceNotFoundException,
      InternalServerException, BadRequestException, AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<GetEnvironmentResponse> responseHandler = protocolFactory.createResponseHandler(operationMetadata,
        GetEnvironmentResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, getEnvironmentRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "GetEnvironment");

      return clientHandler.execute(new ClientExecutionParams<GetEnvironmentRequest, GetEnvironmentResponse>()
          .withOperationName("GetEnvironment").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(getEnvironmentRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new GetEnvironmentRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public GetHostedConfigurationVersionResponse getHostedConfigurationVersion(
      GetHostedConfigurationVersionRequest getHostedConfigurationVersionRequest) throws BadRequestException,
      ResourceNotFoundException, InternalServerException, AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(false).build();

    HttpResponseHandler<GetHostedConfigurationVersionResponse> responseHandler = protocolFactory.createResponseHandler(
        operationMetadata, GetHostedConfigurationVersionResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration,
        getHostedConfigurationVersionRequest.overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "GetHostedConfigurationVersion");

      return clientHandler
          .execute(new ClientExecutionParams<GetHostedConfigurationVersionRequest, GetHostedConfigurationVersionResponse>()
              .withOperationName("GetHostedConfigurationVersion").withResponseHandler(responseHandler)
              .withErrorResponseHandler(errorResponseHandler).withInput(getHostedConfigurationVersionRequest)
              .withMetricCollector(apiCallMetricCollector)
              .withMarshaller(new GetHostedConfigurationVersionRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public ListApplicationsResponse listApplications(ListApplicationsRequest listApplicationsRequest)
      throws InternalServerException, BadRequestException, AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<ListApplicationsResponse> responseHandler = protocolFactory.createResponseHandler(operationMetadata,
        ListApplicationsResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, listApplicationsRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "ListApplications");

      return clientHandler.execute(new ClientExecutionParams<ListApplicationsRequest, ListApplicationsResponse>()
          .withOperationName("ListApplications").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(listApplicationsRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new ListApplicationsRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public ListApplicationsIterable listApplicationsPaginator(ListApplicationsRequest listApplicationsRequest)
      throws InternalServerException, BadRequestException, AwsServiceException, SdkClientException, AppConfigException {
    return new ListApplicationsIterable(this, applyPaginatorUserAgent(listApplicationsRequest));
  }


  @Override
  public ListConfigurationProfilesResponse listConfigurationProfiles(
      ListConfigurationProfilesRequest listConfigurationProfilesRequest) throws ResourceNotFoundException,
      InternalServerException, BadRequestException, AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<ListConfigurationProfilesResponse> responseHandler = protocolFactory.createResponseHandler(
        operationMetadata, ListConfigurationProfilesResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, listConfigurationProfilesRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "ListConfigurationProfiles");

      return clientHandler
          .execute(new ClientExecutionParams<ListConfigurationProfilesRequest, ListConfigurationProfilesResponse>()
              .withOperationName("ListConfigurationProfiles").withResponseHandler(responseHandler)
              .withErrorResponseHandler(errorResponseHandler).withInput(listConfigurationProfilesRequest)
              .withMetricCollector(apiCallMetricCollector)
              .withMarshaller(new ListConfigurationProfilesRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public ListConfigurationProfilesIterable listConfigurationProfilesPaginator(
      ListConfigurationProfilesRequest listConfigurationProfilesRequest) throws ResourceNotFoundException,
      InternalServerException, BadRequestException, AwsServiceException, SdkClientException, AppConfigException {
    return new ListConfigurationProfilesIterable(this, applyPaginatorUserAgent(listConfigurationProfilesRequest));
  }


  @Override
  public ListDeploymentStrategiesResponse listDeploymentStrategies(
      ListDeploymentStrategiesRequest listDeploymentStrategiesRequest) throws InternalServerException, BadRequestException,
      AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<ListDeploymentStrategiesResponse> responseHandler = protocolFactory.createResponseHandler(
        operationMetadata, ListDeploymentStrategiesResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, listDeploymentStrategiesRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "ListDeploymentStrategies");

      return clientHandler
          .execute(new ClientExecutionParams<ListDeploymentStrategiesRequest, ListDeploymentStrategiesResponse>()
              .withOperationName("ListDeploymentStrategies").withResponseHandler(responseHandler)
              .withErrorResponseHandler(errorResponseHandler).withInput(listDeploymentStrategiesRequest)
              .withMetricCollector(apiCallMetricCollector)
              .withMarshaller(new ListDeploymentStrategiesRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public ListDeploymentStrategiesIterable listDeploymentStrategiesPaginator(
      ListDeploymentStrategiesRequest listDeploymentStrategiesRequest) throws InternalServerException, BadRequestException,
      AwsServiceException, SdkClientException, AppConfigException {
    return new ListDeploymentStrategiesIterable(this, applyPaginatorUserAgent(listDeploymentStrategiesRequest));
  }


  @Override
  public ListDeploymentsResponse listDeployments(ListDeploymentsRequest listDeploymentsRequest)
      throws ResourceNotFoundException, InternalServerException, BadRequestException, AwsServiceException,
      SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<ListDeploymentsResponse> responseHandler = protocolFactory.createResponseHandler(operationMetadata,
        ListDeploymentsResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, listDeploymentsRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "ListDeployments");

      return clientHandler.execute(new ClientExecutionParams<ListDeploymentsRequest, ListDeploymentsResponse>()
          .withOperationName("ListDeployments").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(listDeploymentsRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new ListDeploymentsRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public ListDeploymentsIterable listDeploymentsPaginator(ListDeploymentsRequest listDeploymentsRequest)
      throws ResourceNotFoundException, InternalServerException, BadRequestException, AwsServiceException,
      SdkClientException, AppConfigException {
    return new ListDeploymentsIterable(this, applyPaginatorUserAgent(listDeploymentsRequest));
  }


  @Override
  public ListEnvironmentsResponse listEnvironments(ListEnvironmentsRequest listEnvironmentsRequest)
      throws ResourceNotFoundException, InternalServerException, BadRequestException, AwsServiceException,
      SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<ListEnvironmentsResponse> responseHandler = protocolFactory.createResponseHandler(operationMetadata,
        ListEnvironmentsResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, listEnvironmentsRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "ListEnvironments");

      return clientHandler.execute(new ClientExecutionParams<ListEnvironmentsRequest, ListEnvironmentsResponse>()
          .withOperationName("ListEnvironments").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(listEnvironmentsRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new ListEnvironmentsRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public ListEnvironmentsIterable listEnvironmentsPaginator(ListEnvironmentsRequest listEnvironmentsRequest)
      throws ResourceNotFoundException, InternalServerException, BadRequestException, AwsServiceException,
      SdkClientException, AppConfigException {
    return new ListEnvironmentsIterable(this, applyPaginatorUserAgent(listEnvironmentsRequest));
  }


  @Override
  public ListHostedConfigurationVersionsResponse listHostedConfigurationVersions(
      ListHostedConfigurationVersionsRequest listHostedConfigurationVersionsRequest) throws BadRequestException,
      ResourceNotFoundException, InternalServerException, AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<ListHostedConfigurationVersionsResponse> responseHandler = protocolFactory.createResponseHandler(
        operationMetadata, ListHostedConfigurationVersionsResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration,
        listHostedConfigurationVersionsRequest.overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "ListHostedConfigurationVersions");

      return clientHandler
          .execute(new ClientExecutionParams<ListHostedConfigurationVersionsRequest, ListHostedConfigurationVersionsResponse>()
              .withOperationName("ListHostedConfigurationVersions").withResponseHandler(responseHandler)
              .withErrorResponseHandler(errorResponseHandler).withInput(listHostedConfigurationVersionsRequest)
              .withMetricCollector(apiCallMetricCollector)
              .withMarshaller(new ListHostedConfigurationVersionsRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public ListHostedConfigurationVersionsIterable listHostedConfigurationVersionsPaginator(
      ListHostedConfigurationVersionsRequest listHostedConfigurationVersionsRequest) throws BadRequestException,
      ResourceNotFoundException, InternalServerException, AwsServiceException, SdkClientException, AppConfigException {
    return new ListHostedConfigurationVersionsIterable(this, applyPaginatorUserAgent(listHostedConfigurationVersionsRequest));
  }


  @Override
  public ListTagsForResourceResponse listTagsForResource(ListTagsForResourceRequest listTagsForResourceRequest)
      throws ResourceNotFoundException, BadRequestException, InternalServerException, AwsServiceException,
      SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<ListTagsForResourceResponse> responseHandler = protocolFactory.createResponseHandler(
        operationMetadata, ListTagsForResourceResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, listTagsForResourceRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "ListTagsForResource");

      return clientHandler.execute(new ClientExecutionParams<ListTagsForResourceRequest, ListTagsForResourceResponse>()
          .withOperationName("ListTagsForResource").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(listTagsForResourceRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new ListTagsForResourceRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public StartDeploymentResponse startDeployment(StartDeploymentRequest startDeploymentRequest) throws BadRequestException,
      ResourceNotFoundException, ConflictException, InternalServerException, AwsServiceException, SdkClientException,
      AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<StartDeploymentResponse> responseHandler = protocolFactory.createResponseHandler(operationMetadata,
        StartDeploymentResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, startDeploymentRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "StartDeployment");

      return clientHandler.execute(new ClientExecutionParams<StartDeploymentRequest, StartDeploymentResponse>()
          .withOperationName("StartDeployment").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(startDeploymentRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new StartDeploymentRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public StopDeploymentResponse stopDeployment(StopDeploymentRequest stopDeploymentRequest) throws ResourceNotFoundException,
      InternalServerException, BadRequestException, AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<StopDeploymentResponse> responseHandler = protocolFactory.createResponseHandler(operationMetadata,
        StopDeploymentResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, stopDeploymentRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "StopDeployment");

      return clientHandler.execute(new ClientExecutionParams<StopDeploymentRequest, StopDeploymentResponse>()
          .withOperationName("StopDeployment").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(stopDeploymentRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new StopDeploymentRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public TagResourceResponse tagResource(TagResourceRequest tagResourceRequest) throws ResourceNotFoundException,
      BadRequestException, InternalServerException, AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<TagResourceResponse> responseHandler = protocolFactory.createResponseHandler(operationMetadata,
        TagResourceResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, tagResourceRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "TagResource");

      return clientHandler.execute(new ClientExecutionParams<TagResourceRequest, TagResourceResponse>()
          .withOperationName("TagResource").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(tagResourceRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new TagResourceRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public UntagResourceResponse untagResource(UntagResourceRequest untagResourceRequest) throws ResourceNotFoundException,
      BadRequestException, InternalServerException, AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<UntagResourceResponse> responseHandler = protocolFactory.createResponseHandler(operationMetadata,
        UntagResourceResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, untagResourceRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "UntagResource");

      return clientHandler.execute(new ClientExecutionParams<UntagResourceRequest, UntagResourceResponse>()
          .withOperationName("UntagResource").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(untagResourceRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new UntagResourceRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public UpdateApplicationResponse updateApplication(UpdateApplicationRequest updateApplicationRequest)
      throws BadRequestException, ResourceNotFoundException, InternalServerException, AwsServiceException,
      SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<UpdateApplicationResponse> responseHandler = protocolFactory.createResponseHandler(operationMetadata,
        UpdateApplicationResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, updateApplicationRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "UpdateApplication");

      return clientHandler.execute(new ClientExecutionParams<UpdateApplicationRequest, UpdateApplicationResponse>()
          .withOperationName("UpdateApplication").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(updateApplicationRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new UpdateApplicationRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public UpdateConfigurationProfileResponse updateConfigurationProfile(
      UpdateConfigurationProfileRequest updateConfigurationProfileRequest) throws BadRequestException,
      ResourceNotFoundException, InternalServerException, AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<UpdateConfigurationProfileResponse> responseHandler = protocolFactory.createResponseHandler(
        operationMetadata, UpdateConfigurationProfileResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, updateConfigurationProfileRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "UpdateConfigurationProfile");

      return clientHandler
          .execute(new ClientExecutionParams<UpdateConfigurationProfileRequest, UpdateConfigurationProfileResponse>()
              .withOperationName("UpdateConfigurationProfile").withResponseHandler(responseHandler)
              .withErrorResponseHandler(errorResponseHandler).withInput(updateConfigurationProfileRequest)
              .withMetricCollector(apiCallMetricCollector)
              .withMarshaller(new UpdateConfigurationProfileRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public UpdateDeploymentStrategyResponse updateDeploymentStrategy(
      UpdateDeploymentStrategyRequest updateDeploymentStrategyRequest) throws BadRequestException,
      ResourceNotFoundException, InternalServerException, AwsServiceException, SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<UpdateDeploymentStrategyResponse> responseHandler = protocolFactory.createResponseHandler(
        operationMetadata, UpdateDeploymentStrategyResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, updateDeploymentStrategyRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "UpdateDeploymentStrategy");

      return clientHandler
          .execute(new ClientExecutionParams<UpdateDeploymentStrategyRequest, UpdateDeploymentStrategyResponse>()
              .withOperationName("UpdateDeploymentStrategy").withResponseHandler(responseHandler)
              .withErrorResponseHandler(errorResponseHandler).withInput(updateDeploymentStrategyRequest)
              .withMetricCollector(apiCallMetricCollector)
              .withMarshaller(new UpdateDeploymentStrategyRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public UpdateEnvironmentResponse updateEnvironment(UpdateEnvironmentRequest updateEnvironmentRequest)
      throws BadRequestException, ResourceNotFoundException, InternalServerException, AwsServiceException,
      SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<UpdateEnvironmentResponse> responseHandler = protocolFactory.createResponseHandler(operationMetadata,
        UpdateEnvironmentResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, updateEnvironmentRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "UpdateEnvironment");

      return clientHandler.execute(new ClientExecutionParams<UpdateEnvironmentRequest, UpdateEnvironmentResponse>()
          .withOperationName("UpdateEnvironment").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(updateEnvironmentRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new UpdateEnvironmentRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }


  @Override
  public ValidateConfigurationResponse validateConfiguration(ValidateConfigurationRequest validateConfigurationRequest)
      throws BadRequestException, ResourceNotFoundException, InternalServerException, AwsServiceException,
      SdkClientException, AppConfigException {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false)
        .isPayloadJson(true).build();

    HttpResponseHandler<ValidateConfigurationResponse> responseHandler = protocolFactory.createResponseHandler(
        operationMetadata, ValidateConfigurationResponse::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(protocolFactory,
        operationMetadata);
    List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, validateConfigurationRequest
        .overrideConfiguration().orElse(null));
    MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector
        .create("ApiCall");
    try {
      apiCallMetricCollector.reportMetric(CoreMetric.SERVICE_ID, "AppConfig");
      apiCallMetricCollector.reportMetric(CoreMetric.OPERATION_NAME, "ValidateConfiguration");

      return clientHandler.execute(new ClientExecutionParams<ValidateConfigurationRequest, ValidateConfigurationResponse>()
          .withOperationName("ValidateConfiguration").withResponseHandler(responseHandler)
          .withErrorResponseHandler(errorResponseHandler).withInput(validateConfigurationRequest)
          .withMetricCollector(apiCallMetricCollector)
          .withMarshaller(new ValidateConfigurationRequestMarshaller(protocolFactory)));
    } finally {
      metricPublishers.forEach(p -> p.publish(apiCallMetricCollector.collect()));
    }
  }

  private static List<MetricPublisher> resolveMetricPublishers(SdkClientConfiguration clientConfiguration,
      RequestOverrideConfiguration requestOverrideConfiguration) {
    List<MetricPublisher> publishers = null;
    if (requestOverrideConfiguration != null) {
      publishers = requestOverrideConfiguration.metricPublishers();
    }
    if (publishers == null || publishers.isEmpty()) {
      publishers = clientConfiguration.option(SdkClientOption.METRIC_PUBLISHERS);
    }
    if (publishers == null) {
      publishers = Collections.emptyList();
    }
    return publishers;
  }

  private HttpResponseHandler<AwsServiceException> createErrorResponseHandler(
      BaseAwsJsonProtocolFactory protocolFactory,
      JsonOperationMetadata operationMetadata) {
    return protocolFactory.createErrorResponseHandler(operationMetadata);
  }

  private <T extends BaseAwsJsonProtocolFactory.Builder<T>> T init(T builder) {
    return builder
        .clientConfiguration(clientConfiguration)
        .defaultServiceExceptionSupplier(AppConfigException::builder)
        .protocol(AwsJsonProtocol.REST_JSON)
        .protocolVersion("1.1")
        .registerModeledException(
            ExceptionMetadata.builder().errorCode("ConflictException")
                .exceptionBuilderSupplier(ConflictException::builder).httpStatusCode(409).build())
        .registerModeledException(
            ExceptionMetadata.builder().errorCode("ResourceNotFoundException")
                .exceptionBuilderSupplier(ResourceNotFoundException::builder).httpStatusCode(404).build())
        .registerModeledException(
            ExceptionMetadata.builder().errorCode("PayloadTooLargeException")
                .exceptionBuilderSupplier(PayloadTooLargeException::builder).httpStatusCode(413).build())
        .registerModeledException(
            ExceptionMetadata.builder().errorCode("ServiceQuotaExceededException")
                .exceptionBuilderSupplier(ServiceQuotaExceededException::builder).httpStatusCode(402).build())
        .registerModeledException(
            ExceptionMetadata.builder().errorCode("InternalServerException")
                .exceptionBuilderSupplier(InternalServerException::builder).httpStatusCode(500).build())
        .registerModeledException(
            ExceptionMetadata.builder().errorCode("BadRequestException")
                .exceptionBuilderSupplier(BadRequestException::builder).httpStatusCode(400).build());
  }

  @Override
  public void close() {
    clientHandler.close();
  }

  private <T extends AppConfigRequest> T applyPaginatorUserAgent(T request) {
    Consumer<AwsRequestOverrideConfiguration.Builder> userAgentApplier = b -> b.addApiName(ApiName.builder()
        .version(VersionInfo.SDK_VERSION).name("PAGINATED").build());
    AwsRequestOverrideConfiguration overrideConfiguration = request.overrideConfiguration()
        .map(c -> c.toBuilder().applyMutation(userAgentApplier).build())
        .orElse((AwsRequestOverrideConfiguration.builder().applyMutation(userAgentApplier).build()));
    return (T) request.toBuilder().overrideConfiguration(overrideConfiguration).build();
  }
}
