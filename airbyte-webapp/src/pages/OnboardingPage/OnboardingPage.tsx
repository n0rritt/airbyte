import React, { Suspense, useEffect, useState } from "react";
import styled from "styled-components";
import { useResource } from "rest-hooks";

import HeadTitle from "components/HeadTitle";
import useSource, {
  useSourceList,
} from "components/hooks/services/useSourceHook";
import useDestination, {
  useDestinationList,
} from "components/hooks/services/useDestinationHook";
import { JobInfo } from "core/resources/Scheduler";
import { ConnectionConfiguration } from "core/domain/connection";
import SourceDefinitionResource from "core/resources/SourceDefinition";
import DestinationDefinitionResource from "core/resources/DestinationDefinition";
import useGetStepsConfig from "./useStepsConfig";
import SourceStep from "./components/SourceStep";
import DestinationStep from "./components/DestinationStep";
import ConnectionStep from "./components/ConnectionStep";
import WelcomeStep from "./components/WelcomeStep";
import { StepType } from "./types";
import { useAnalytics } from "components/hooks/useAnalytics";
import StepsCounter from "./components/StepsCounter";
import LoadingPage from "components/LoadingPage";

const Content = styled.div<{ big?: boolean; medium?: boolean }>`
  width: 100%;
  max-width: ${({ big, medium }) => (big ? 1140 : medium ? 730 : 550)}px;
  margin: 0 auto;
  padding: 75px 0 30px;
  display: flex;
  flex-direction: column;
  align-items: center;
  min-height: 100%;
`;

const OnboardingPage: React.FC = () => {
  const analyticsService = useAnalytics();

  useEffect(() => {
    analyticsService.page("Onboarding Page");
  }, []);

  const { sources } = useSourceList();
  const { destinations } = useDestinationList();

  const { sourceDefinitions } = useResource(
    SourceDefinitionResource.listShape(),
    {}
  );
  const { destinationDefinitions } = useResource(
    DestinationDefinitionResource.listShape(),
    {}
  );

  const { createSource, recreateSource } = useSource();
  const { createDestination, recreateDestination } = useDestination();

  const [successRequest, setSuccessRequest] = useState(false);
  const [errorStatusRequest, setErrorStatusRequest] = useState<{
    status: number;
    response: JobInfo;
    message: string;
  } | null>(null);

  const afterUpdateStep = () => {
    setSuccessRequest(false);
    setErrorStatusRequest(null);
  };

  const { currentStep, setCurrentStep, steps } = useGetStepsConfig(
    !!sources.length,
    !!destinations.length,
    afterUpdateStep
  );

  const getSourceDefinitionById = (id: string) =>
    sourceDefinitions.find((item) => item.sourceDefinitionId === id);

  const getDestinationDefinitionById = (id: string) =>
    destinationDefinitions.find((item) => item.destinationDefinitionId === id);

  const renderStep = () => {
    if (currentStep === StepType.INSTRUCTION) {
      const onStart = () => setCurrentStep(StepType.CREATE_SOURCE);
      //TODO: add username
      return <WelcomeStep onSubmit={onStart} userName="" />;
    }
    if (currentStep === StepType.CREATE_SOURCE) {
      const onSubmitSourceStep = async (values: {
        name: string;
        serviceType: string;
        sourceId?: string;
        connectionConfiguration?: ConnectionConfiguration;
      }) => {
        setErrorStatusRequest(null);
        const sourceConnector = getSourceDefinitionById(values.serviceType);

        try {
          if (!!sources.length) {
            await recreateSource({
              values,
              sourceId: sources[0].sourceId,
            });
          } else {
            await createSource({ values, sourceConnector });
          }

          setSuccessRequest(true);
          setTimeout(() => {
            setSuccessRequest(false);
            setCurrentStep(StepType.CREATE_DESTINATION);
          }, 2000);
        } catch (e) {
          setErrorStatusRequest(e);
        }
      };
      return (
        <SourceStep
          afterSelectConnector={() => setErrorStatusRequest(null)}
          jobInfo={errorStatusRequest?.response}
          onSubmit={onSubmitSourceStep}
          availableServices={sourceDefinitions}
          hasSuccess={successRequest}
          error={errorStatusRequest}
          // source={sources.length && !successRequest ? sources[0] : undefined}
        />
      );
    }
    if (currentStep === StepType.CREATE_DESTINATION) {
      const onSubmitDestinationStep = async (values: {
        name: string;
        serviceType: string;
        destinationDefinitionId?: string;
        connectionConfiguration?: ConnectionConfiguration;
      }) => {
        setErrorStatusRequest(null);
        const destinationConnector = getDestinationDefinitionById(
          values.serviceType
        );

        try {
          if (!!destinations.length) {
            await recreateDestination({
              values,
              destinationId: destinations[0].destinationId,
            });
          } else {
            await createDestination({
              values,
              destinationConnector,
            });
          }

          setSuccessRequest(true);
          setTimeout(() => {
            setSuccessRequest(false);
            setCurrentStep(StepType.SET_UP_CONNECTION);
          }, 2000);
        } catch (e) {
          setErrorStatusRequest(e);
        }
      };
      return (
        <DestinationStep
          afterSelectConnector={() => setErrorStatusRequest(null)}
          jobInfo={errorStatusRequest?.response}
          onSubmit={onSubmitDestinationStep}
          availableServices={destinationDefinitions}
          hasSuccess={successRequest}
          error={errorStatusRequest}
          currentSourceDefinitionId={sources[0].sourceDefinitionId}
          // destination={
          //   destinations.length && !successRequest ? destinations[0] : undefined
          // }
        />
      );
    }

    return (
      <ConnectionStep
        errorStatus={errorStatusRequest?.status}
        source={sources[0]}
        destination={destinations[0]}
      />
    );
  };

  return (
    <Content
      big={currentStep === StepType.SET_UP_CONNECTION}
      medium={
        currentStep === StepType.INSTRUCTION || currentStep === StepType.FINAl
      }
    >
      <HeadTitle titles={[{ id: "onboarding.headTitle" }]} />
      <StepsCounter steps={steps} currentStep={currentStep} />

      <Suspense fallback={<LoadingPage />}>{renderStep()}</Suspense>
    </Content>
  );
};

export default OnboardingPage;
