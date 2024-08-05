
package org.whispersystems.textsecuregcm.experiment;

import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import reactor.core.publisher.Flux;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * A push notification selects for eligible devices, applies a control or experimental treatment, and provides a
 * mechanism for comparing device states before and after receiving the treatment.
 *
 * @param <T> the type of state object stored for this experiment
 */
public interface PushNotificationExperiment<T> {

  /**
   * Returns the unique name of this experiment.
   *
   * @return the unique name of this experiment
   */
  String getExperimentName();

  /**
   * Tests whether a device is eligible for this experiment. An eligible device may be assigned to either the control
   * or experiment group within an experiment. Ineligible devices will not participate in the experiment in any way.
   *
   * @param account the account to which the device belongs
   * @param device the device to test for eligibility in this experiment
   *
   * @return a future that yields a boolean value indicating whether the target device is eligible for this experiment
   */
  CompletableFuture<Boolean> isDeviceEligible(Account account, Device device);

  /**
   * Returns the class of the state object stored for this experiment.
   *
   * @return the class of the state object stored for this experiment
   */
  Class<T> getStateClass();

  /**
   * Generates an experiment specific state "snapshot" of the given device. Experiment results are generally evaluated
   * by comparing a device's state before a treatment is applied and its state after the treatment is applied.
   *
   * @param account the account to which the device belongs
   * @param device the device for which to generate a state "snapshot"
   *
   * @return an experiment-specific state "snapshot" of the given device
   */
  T getState(@Nullable Account account, @Nullable Device device);

  /**
   * Applies a control treatment to the given device. In many cases (and by default) no action is taken for devices in
   * the control group.
   *
   * @param account the account to which the device belongs
   * @param device the device to which to apply the control treatment for this experiment
   *
   * @return a future that completes when the control treatment has been applied for the given device
   */
  default CompletableFuture<Void> applyControlTreatment(Account account, Device device) {
    return CompletableFuture.completedFuture(null);
  };

  /**
   * Applies an experimental treatment to the given device. This generally involves sending or scheduling a specific
   * type of push notification for the given device.
   *
   * @param account the account to which the device belongs
   * @param device the device to which to apply the experimental treatment for this experiment
   *
   * @return a future that completes when the experimental treatment has been applied for the given device
   */
  CompletableFuture<Void> applyExperimentTreatment(Account account, Device device);

  /**
   * Consumes a stream of finished samples and emits an analysis of the results via an implementation-specific channel
   * (e.g. a log message). Implementations must block until all samples have been consumed and analyzed.
   *
   * @param samples a stream of finished samples from this experiment
   */
  void analyzeResults(Flux<PushNotificationExperimentSample<T>> samples);
}
